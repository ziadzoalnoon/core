import { State } from "@arkecosystem/core-interfaces";
import { Wallets } from "@arkecosystem/core-state";
import { Handlers } from "@arkecosystem/core-transactions";
import { Crypto, Errors, Interfaces, Managers, Transactions } from "@arkecosystem/crypto";
import debounceFn, { DebouncedFunction } from "debounce-fn";
import PQueue from 'p-queue';
import { expose } from "threads";
import { Observable, Subject } from "threads/observable";
import {
    IPendingTransactionJobResult,
    ITransactionWorkerJob
} from './types';
import { pushError } from './utils';

export class PoolWorker {
    private options: Record<string, any>;
    private observable: Subject<IPendingTransactionJobResult>;
    private queue: PQueue = new PQueue({ concurrency: 1 });

    private jobResultDebouncer: DebouncedFunction<any, any>;
    private finishedJobs: IPendingTransactionJobResult[];
    public constructor() {
        this.observable = new Subject();

        this.finishedJobs = [];
        this.jobResultDebouncer = debounceFn(async () => {
            if (this.finishedJobs.length > 0) {
                this.observable.next(this.finishedJobs.shift());
            }

            await this.queue.onIdle();
            this.jobResultDebouncer();

        }, { wait: 50, immediate: true });

        console.log("Started PoolWorker.");
    }

    public getObservable(): Observable<IPendingTransactionJobResult> {
        return Observable.from(this.observable);
    }

    public createJob(job: ITransactionWorkerJob): void {
        this.queue.add(async () => {
            try {
                return this.processJob(job);
            } catch (error) {
                console.log(error.stack);
            }
        });
    }

    public configure(options: any): void {
        this.options = options;
        Managers.configManager.setFromPreset(this.options.networkName);
        this.updateBlockHeight(this.options.lastHeight);
    }

    public updateBlockHeight(height: number): void {
        Managers.configManager.setHeight(height);
    }

    private async processJob(job: ITransactionWorkerJob): Promise<void> {
        const { transactions, senderWallets } = job;

        const result: IPendingTransactionJobResult = {
            ticketId: job.ticketId,
            validTransactions: [],
            invalid: {},
            excess: {},
            errors: {},
        };

        for (const transactionData of transactions) {
            try {
                if (!this.performBasicTransactionChecks(result, transactionData)) {
                    continue;
                }

                const transaction: Interfaces.ITransaction = Transactions.TransactionFactory.fromData(transactionData);
                const handler: Handlers.TransactionHandler = Handlers.Registry.get(
                    transaction.type,
                    transaction.typeGroup,
                );

                const walletData: State.IWallet = senderWallets[transactionData.senderPublicKey];
                const senderWallet: State.IWallet = Object.assign(new Wallets.Wallet(walletData.address), {
                    ...walletData,
                });

                if (!(await handler.verify(transaction, senderWallet))) {

                    pushError(result, transactionData.id, {
                        type: "ERR_BAD_DATA",
                        message: "Failed to verify transaction signature.",
                    });

                    continue;
                }

                result.validTransactions.push({ buffer: transaction.serialized, id: transaction.id });
            } catch (error) {
                console.log(error.stack);
                if (error instanceof Errors.TransactionSchemaError) {
                    pushError(result, transactionData.id, {
                        type: "ERR_TRANSACTION_SCHEMA",
                        message: error.message,
                    });

                } else {
                    pushError(result, transactionData.id, {
                        type: "ERR_UNKNOWN",
                        message: error.message,
                    });
                }
            }
        }

        this.finishedJobs.push(result);
        this.jobResultDebouncer();
    }

    private performBasicTransactionChecks(result: IPendingTransactionJobResult, transaction: Interfaces.ITransactionData): boolean {
        const now: number = Crypto.Slots.getTime();
        const lastHeight: number = Managers.configManager.getHeight();
        const maxTransactionBytes: number = this.options.maxTransactionBytes;

        if (transaction.timestamp > now + 3600) {
            const secondsInFuture: number = transaction.timestamp - now;

            pushError(result, transaction.id, {
                type: "ERR_FROM_FUTURE",
                message: `Transaction ${transaction.id} is ${secondsInFuture} seconds in the future`
            });

            return false;

        } else if (transaction.expiration > 0 && transaction.expiration <= lastHeight + 1) {
            pushError(result, transaction.id, {
                type: "ERR_EXPIRED",
                message: `Transaction ${transaction.id} is expired since ${lastHeight -
                    transaction.expiration} blocks.`
            });

            return false;

        } else if (transaction.network && transaction.network !== Managers.configManager.get("network.pubKeyHash")) {
            pushError(result, transaction.id, {
                type: "ERR_WRONG_NETWORK",
                message: `Transaction network '${transaction.network}' does not match '${Managers.configManager.get(
                    "pubKeyHash",
                )}'`
            });

            return false;

        } else if (JSON.stringify(transaction).length > maxTransactionBytes) {
            // TODO: still needed ?
            pushError(result, transaction.id, {
                type: "ERR_TOO_LARGE",
                message: `Transaction ${transaction.id} is larger than ${maxTransactionBytes} bytes.`,
            });

            return false;
        }

        return true;
    }
}

const poolWorker = new PoolWorker();

export type WorkerApi = Pick<PoolWorker, "configure" | "createJob" | "getObservable" | "updateBlockHeight">

const workerApi: WorkerApi = {
    configure: (options: any) => poolWorker.configure(options),
    createJob: (job: ITransactionWorkerJob) => poolWorker.createJob(job),
    getObservable: () => poolWorker.getObservable(),
    updateBlockHeight: (height: number) => poolWorker.updateBlockHeight(height),
}

expose(workerApi);