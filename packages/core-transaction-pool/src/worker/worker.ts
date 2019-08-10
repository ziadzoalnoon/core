import { State } from "@arkecosystem/core-interfaces";
import { Wallets } from "@arkecosystem/core-state";
import { Handlers } from "@arkecosystem/core-transactions";
import { Crypto, Errors, Interfaces, Managers, Transactions } from "@arkecosystem/crypto";
import async from "async";
import { delay } from "bluebird";
import { expose } from "threads";
import { Observable, Subject } from "threads/observable";
import uuidv4 from "uuid/v4";
import { parentPort } from "worker_threads";
import {
    IPendingTransactionJobResult,
    IQueuedTransactionJob, ITransactionWorkerJob
} from './types';
import { pushError } from './utils';

export class PoolWorker {
    private results: Subject<IPendingTransactionJobResult>;
    private options: Record<string, any>;
    private queue: async.AsyncQueue<{ job: IQueuedTransactionJob }>;

    public constructor() {
        this.results = new Subject();

        this.queue = async.queue(({ job }: { job: IQueuedTransactionJob }, cb) => {
            delay(1)
                .then(() => {
                    try {
                        return this.processTransactions(job, cb);
                    } catch (error) {
                        console.log(error.stack);
                        return cb();
                    }
                })
                .catch(error => {
                    console.log(error.stack);
                    return cb();
                });
        });

        this.queue.drain(() => console.log("Transactions queue empty."));

        console.log("Started PoolWorker.");

        parentPort.on("message", message => {
            // console.log(message);
        });
    }

    public getObservable(): Observable<IPendingTransactionJobResult> {
        return Observable.from(this.results);
    }

    public createJob(job: ITransactionWorkerJob): string {
        const ticketId: string = uuidv4();
        this.queue.push({ job: { ...job, ticketId } });
        return ticketId;
    }

    public configure(options: any): void {
        this.options = options;
        Managers.configManager.setFromPreset(this.options.networkName);
        this.updateBlockHeight(this.options.lastHeight);
    }

    public updateBlockHeight(height: number): void {
        Managers.configManager.setHeight(height);
    }

    private async processTransactions(job: IQueuedTransactionJob, cb: any): Promise<void> {
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

        this.results.next(result);
        return cb(result);
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