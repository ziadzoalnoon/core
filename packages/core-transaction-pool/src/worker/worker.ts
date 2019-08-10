import { State } from "@arkecosystem/core-interfaces";
import { Wallets } from "@arkecosystem/core-state";
import { Handlers } from "@arkecosystem/core-transactions";
import { Crypto, Errors, Interfaces, Managers, Transactions } from "@arkecosystem/crypto";
import async from "async";
import { delay } from "bluebird";
import uuidv4 from "uuid/v4";
import { parentPort } from "worker_threads";
import {
    BrokerToWorker, IMessageObject, IPendingTransactionJobResult,
    IQueuedTransactionJob, ITransactionWorkerJob, WorkerToBroker
} from './types';
import { pushError } from './utils';

export class PoolWorker {
    private options: Record<string, any>;
    private queue: async.AsyncQueue<{ job: IQueuedTransactionJob }>;

    public constructor() {
        this.queue = async.queue(({ job }: { job: IQueuedTransactionJob }, cb) => {
            const { transactions } = job;
            console.log("queue processing: " + transactions.length);
            delay(100)
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

        parentPort.on("message", (message: IMessageObject<BrokerToWorker>) => {
            console.log("Received message: " + message.type);

            switch (message.type) {
                case BrokerToWorker.Initialize: {
                    this.options = message.data;
                    console.log("Setting network: " + this.options.networkName);
                    Managers.configManager.setFromPreset(this.options.networkName);
                    break;
                }

                case BrokerToWorker.CreateJob: {
                    const ticketId: string = this.addTransactionsToQueue(message.data);
                    this.sendToBroker(WorkerToBroker.TicketId, ticketId);
                    break;
                }

                case BrokerToWorker.BlockHeightUpdate: {
                    console.log("Setting height to: " + message.data);
                    Managers.configManager.setHeight(message.data);
                    break;
                }

                default: {
                    console.log("Unknown message: " + message.type);
                    this.sendToBroker(WorkerToBroker.Unknown, message.type);
                }
            }
        });
    }

    private addTransactionsToQueue(job: ITransactionWorkerJob): string {
        const ticketId: string = uuidv4();
        this.queue.push({ job: { ...job, ticketId } });
        return ticketId;
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

        console.log("Processed " + result.validTransactions.length + " valid transactions.");
        this.sendToBroker(WorkerToBroker.TransactionJobResult, result);

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

    private sendToBroker(type: WorkerToBroker, data: any): void {
        parentPort.postMessage({ type, data });
    }
}

// tslint:disable-next-line: no-unused-expression
new PoolWorker();

const keepAlive = async () => {
    return delay(50000);
};

(async () => {
    await keepAlive();
})();
