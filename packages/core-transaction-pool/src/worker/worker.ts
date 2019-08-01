import { State } from "@arkecosystem/core-interfaces";
import { Wallets } from "@arkecosystem/core-state";
import { Handlers } from "@arkecosystem/core-transactions";
import { Crypto, Errors, Interfaces, Managers, Transactions } from "@arkecosystem/crypto";
import async from "async";
import { delay } from "bluebird";
import uuidv4 from "uuid/v4";
import { parentPort } from "worker_threads";
import { BrokerToWorker, WorkerToBroker } from "./pool-broker";

export interface IMessageObject<T> {
    type: T;
    data: any;
}

export interface ITransactionValidationError {
    error: string;
    message: string;
}

export interface ITransactionsWorkerJob {
    transactions: ReadonlyArray<Interfaces.ITransactionData>;
    senderWallets: Record<string, State.IWallet>;
}

export interface ITransactionJobWorkerResult {
    ticketId: string;
    validTransactions: Buffer[];
    invalidTransactions: Record<string, ITransactionValidationError>;
}

export interface IFinishedTransactionJobResult {
    ticketId: string;
    validTransactions: Interfaces.ITransaction[];
    invalidTransactions: Record<string, ITransactionValidationError>;
}

export interface IQueuedTransactionsJob extends ITransactionsWorkerJob {
    ticketId: string;
}

export class PoolWorker {
    private options: Record<string, any>;
    private queue: async.AsyncQueue<{ job: IQueuedTransactionsJob }>;

    public constructor() {
        this.queue = async.queue(({ job }: { job: IQueuedTransactionsJob }, cb) => {
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

    private addTransactionsToQueue(job: ITransactionsWorkerJob): string {
        const ticketId: string = uuidv4();
        this.queue.push({ job: { ...job, ticketId } });
        return ticketId;
    }

    private async processTransactions(job: IQueuedTransactionsJob, cb: any): Promise<void> {
        const { transactions, senderWallets } = job;

        const response: ITransactionJobWorkerResult = {
            ticketId: job.ticketId,
            validTransactions: [],
            invalidTransactions: {},
        };

        for (const transactionData of transactions) {
            console.log("tx: " + transactionData.id);
            try {
                const result = this.performPrimitiveTransactionChecks(transactionData);
                if (result.error !== undefined) {
                    response.invalidTransactions[transactionData.id] = result;
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
                    response.invalidTransactions[transactionData.id] = {
                        error: "ERR_BAD_DATA",
                        message: "Failed to verify transaction signature.",
                    };

                    continue;
                }

                response.validTransactions.push(transaction.serialized);
            } catch (error) {
                console.log("=!=!=!=!");
                console.log(error.stack);
                if (error instanceof Errors.TransactionSchemaError) {
                    response.invalidTransactions[transactionData.id] = {
                        error: "ERR_TRANSACTION_SCHEMA",
                        message: error.message,
                    };
                } else {
                    response.invalidTransactions[transactionData.id] = {
                        error: "ERR_UNKNOWN",
                        message: error.message,
                    };
                }
            }
        }

        console.log("Processed " + response.validTransactions.length + " valid transactions.");
        this.sendToBroker(WorkerToBroker.TransactionJobResult, response);

        return cb(response);
    }

    private performPrimitiveTransactionChecks(transaction: Interfaces.ITransactionData): ITransactionValidationError {
        const result: ITransactionValidationError = {
            error: undefined,
            message: undefined,
        };

        const now: number = Crypto.Slots.getTime();
        const lastHeight: number = Managers.configManager.getHeight();
        const maxTransactionBytes: number = this.options.maxTransactionBytes;

        if (transaction.timestamp > now + 3600) {
            const secondsInFuture: number = transaction.timestamp - now;
            result.error = "ERR_FROM_FUTURE";
            result.message = `Transaction ${transaction.id} is ${secondsInFuture} seconds in the future`;
        } else if (transaction.expiration > 0 && transaction.expiration <= lastHeight + 1) {
            result.error = "ERR_EXPIRED";
            result.message = `Transaction ${transaction.id} is expired since ${lastHeight -
                transaction.expiration} blocks.`;
        } else if (transaction.network && transaction.network !== Managers.configManager.get("network.pubKeyHash")) {
            result.error = "ERR_WRONG_NETWORK";
            result.message = `Transaction network '${transaction.network}' does not match '${Managers.configManager.get(
                "pubKeyHash",
            )}'`;
        } else if (JSON.stringify(transaction).length > maxTransactionBytes) {
            // TODO: still needed ?
            result.error = "ERR_TOO_LARGE";
            result.message = `Transaction ${transaction.id} is larger than ${maxTransactionBytes} bytes.`;
        }

        return result;
    }

    private sendToBroker<T>(type: WorkerToBroker, data: T): void {
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
