import { app } from "@arkecosystem/core-container";
import { Database, Logger, State, TransactionPool } from "@arkecosystem/core-interfaces";
import { Errors, Handlers } from "@arkecosystem/core-transactions";
import { Enums, Interfaces, Transactions } from "@arkecosystem/crypto";
import assert from "assert";
import async from "async";
import { delay } from 'bluebird';
import pluralize from "pluralize";
import { dynamicFeeMatcher } from "./dynamic-fee";
import { IDynamicFeeMatch, ITransactionsCached, ITransactionsProcessed } from "./interfaces";
import { WalletManager } from "./wallet-manager";
import { BrokerToWorker, PoolBroker } from './worker/pool-broker';
import { IFinishedTransactionJobResult, ITransactionJobWorkerResult } from './worker/worker';

export class ProcessorV2 {
    private pendingTickets: Map<string, boolean> = new Map();
    private processedTickets: Map<string, IFinishedTransactionJobResult> = new Map();

    // @ts-ignore
    private readonly poolBroker: PoolBroker;
    private readonly queue: async.AsyncQueue<{ job: ITransactionJobWorkerResult }>;

    public constructor(private readonly pool: TransactionPool.IConnection, private readonly walletManager: WalletManager) {

        try {
            this.poolBroker = new PoolBroker(this);
        } catch (ex) {
            console.log(ex.message);
        }

        this.queue = async.queue(({ job }: { job: ITransactionJobWorkerResult }, cb) => {
            const { ticketId, validTransactions, } = job;
            console.log("received ticket " + ticketId + " with " + validTransactions.length + " valid transactions from worker");

            delay(100)
                .then(() => {
                    try {
                        return this.finishTransactionJob(job, cb);
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

    }

    public async createTransactionsJob(transactions: Interfaces.ITransactionData[]): Promise<string> {
        // TODO: cache transactions

        const eligibleTransactions: Interfaces.ITransactionData[] = [];
        const senderWallets: Record<string, State.IWallet> = {};
        for (const transaction of transactions) {
            // TODO: optimize
            if (!(await this.preWorkerChecks(transaction))) {
                continue;
            }

            const senderWallet: State.IWallet = this.walletManager.findByPublicKey(transaction.senderPublicKey);
            senderWallets[transaction.senderPublicKey] = senderWallet;

            // TODO: need to convert bignum back to string, fixme properly,
            (transaction.amount as any) = transaction.amount.toFixed();
            (transaction.fee as any) = transaction.fee.toFixed();

            eligibleTransactions.push(transaction);
        }

        const ticketId: string = await this.poolBroker.sendToWorker(BrokerToWorker.CreateJob, {
            transactions,
            senderWallets,
        });

        this.pendingTickets.set(ticketId, true);

        return ticketId;
    }

    public addFinishedWorkerJobToQueue(job: ITransactionJobWorkerResult): void {
        this.queue.push({ job });
    }

    private async finishTransactionJob(workerJobResult: ITransactionJobWorkerResult, cb: any): Promise<void> {
        const { ticketId, validTransactions, invalidTransactions } = workerJobResult;

        const jobResult: IFinishedTransactionJobResult = {
            ticketId,
            validTransactions: [],
            invalidTransactions,
        }

        for (const transactionBuffer of validTransactions) {
            try {
                const transaction: Interfaces.ITransaction = Transactions.TransactionFactory.fromBytesUnsafe(transactionBuffer);

                try {
                    assert(transaction.isVerified);
                    await this.walletManager.throwIfCannotBeApplied(transaction);
                    const dynamicFee: IDynamicFeeMatch = dynamicFeeMatcher(transaction);
                    if (!dynamicFee.enterPool && !dynamicFee.broadcast) {
                        jobResult.invalidTransactions[transaction.id] = {
                            error: "ERR_LOW_FEE",
                            message: "The fee is too low to broadcast and accept the transaction",
                        };

                    } else {
                        // if (dynamicFee.enterPool) {
                        //     this.accept.set(transactionInstance.data.id, transactionInstance);
                        // }

                        // if (dynamicFee.broadcast) {
                        //     this.broadcast.set(transactionInstance.data.id, transactionInstance);
                        // }
                    }

                    const { notAdded } = await this.pool.addTransactions([transaction]);
                    if (notAdded.length > 0) {
                        jobResult.invalidTransactions[transaction.id] = {
                            error: notAdded[0].type,
                            message: notAdded[0].message,
                        }

                        continue;
                    }

                    jobResult.validTransactions.push(transaction);

                } catch (error) {
                    jobResult.invalidTransactions[transaction.id] = {
                        error: "ERR_APPLY",
                        message: error.message,
                    };
                }
            } catch (error) {
                console.log("Bad bad error: " + error.message);
            }
        }

        this.pendingTickets.delete(jobResult.ticketId);
        this.processedTickets.set(jobResult.ticketId, jobResult);

        return cb();
    }

    private async preWorkerChecks(transaction: Interfaces.ITransactionData): Promise<boolean> {
        try {

            if (await this.pool.has(transaction.id)) {
                return false;
            }

            if (await this.pool.hasExceededMaxTransactions(transaction.senderPublicKey)) {
                return false;
            }

            const handler: Handlers.TransactionHandler = Handlers.Registry.get(transaction.type, transaction.typeGroup);

            if (!(await handler.canEnterTransactionPool(transaction, this.pool, undefined))) {
                return false;
            }

            return true;

        } catch {
            return false;
        }
    }

}


/**
 * @TODO: this class has too many responsibilities at the moment.
 * Its sole responsibility should be to validate transactions and return them.
 */
// tslint:disable-next-line: max-classes-per-file
export class Processor implements TransactionPool.IProcessor {
    private transactions: Interfaces.ITransactionData[] = [];
    private readonly excess: string[] = [];
    private readonly accept: Map<string, Interfaces.ITransaction> = new Map();
    private readonly broadcast: Map<string, Interfaces.ITransaction> = new Map();
    private readonly invalid: Map<string, Interfaces.ITransactionData> = new Map();
    private readonly errors: { [key: string]: TransactionPool.ITransactionErrorResponse[] } = {};

    constructor(private readonly pool: TransactionPool.IConnection, private readonly walletManager: WalletManager) { }

    public async validate(transactions: Interfaces.ITransactionData[]): Promise<TransactionPool.IProcessorResult> {
        this.cacheTransactions(transactions);

        if (this.transactions.length > 0) {
            await this.filterAndTransformTransactions(this.transactions);

            await this.removeForgedTransactions();

            await this.addTransactionsToPool();

            this.printStats();
        }

        return {
            accept: Array.from(this.accept.keys()),
            broadcast: Array.from(this.broadcast.keys()),
            invalid: Array.from(this.invalid.keys()),
            excess: this.excess,
            errors: Object.keys(this.errors).length > 0 ? this.errors : undefined,
        };
    }

    public getTransactions(): Interfaces.ITransactionData[] {
        return this.transactions;
    }

    public getBroadcastTransactions(): Interfaces.ITransaction[] {
        return Array.from(this.broadcast.values());
    }

    public getErrors(): { [key: string]: TransactionPool.ITransactionErrorResponse[] } {
        return this.errors;
    }

    public pushError(transaction: Interfaces.ITransactionData, type: string, message: string): void {
        if (!this.errors[transaction.id]) {
            this.errors[transaction.id] = [];
        }

        this.errors[transaction.id].push({ type, message });

        this.invalid.set(transaction.id, transaction);
    }

    // TODO: can maybe replaced with something nicer now
    private cacheTransactions(transactions: Interfaces.ITransactionData[]): void {
        const { added, notAdded }: ITransactionsCached = app
            .resolvePlugin<State.IStateService>("state")
            .getStore()
            .cacheTransactions(transactions);

        this.transactions = added;

        for (const transaction of notAdded) {
            if (!this.errors[transaction.id]) {
                this.pushError(transaction, "ERR_DUPLICATE", "Already in cache.");
            }
        }
    }

    private async removeForgedTransactions(): Promise<void> {
        const forgedIdsSet: string[] = await app
            .resolvePlugin<Database.IDatabaseService>("database")
            .getForgedTransactionsIds([...new Set([...this.accept.keys(), ...this.broadcast.keys()])]);

        app.resolvePlugin<State.IStateService>("state")
            .getStore()
            .removeCachedTransactionIds(forgedIdsSet);

        for (const id of forgedIdsSet) {
            this.pushError(this.accept.get(id).data, "ERR_FORGED", "Already forged.");

            this.accept.delete(id);
            this.broadcast.delete(id);
        }
    }

    private async filterAndTransformTransactions(transactions: Interfaces.ITransactionData[]): Promise<void> {
        for (const transaction of transactions) {
            const exists: boolean = await this.pool.has(transaction.id);

            if (exists) {
                this.pushError(transaction, "ERR_DUPLICATE", `Duplicate transaction ${transaction.id}`);
            } else if (await this.pool.hasExceededMaxTransactions(transaction.senderPublicKey)) {
                this.excess.push(transaction.id);
            } else if (await this.validateTransaction(transaction)) {
                try {
                    const transactionInstance: Interfaces.ITransaction = Transactions.TransactionFactory.fromData(
                        transaction,
                    );
                    try {
                        await this.walletManager.throwIfCannotBeApplied(transactionInstance);
                        const dynamicFee: IDynamicFeeMatch = dynamicFeeMatcher(transactionInstance);
                        if (!dynamicFee.enterPool && !dynamicFee.broadcast) {
                            this.pushError(
                                transaction,
                                "ERR_LOW_FEE",
                                "The fee is too low to broadcast and accept the transaction",
                            );
                        } else {
                            if (dynamicFee.enterPool) {
                                this.accept.set(transactionInstance.data.id, transactionInstance);
                            }

                            if (dynamicFee.broadcast) {
                                this.broadcast.set(transactionInstance.data.id, transactionInstance);
                            }
                        }
                    } catch (error) {
                        this.pushError(transaction, "ERR_APPLY", error.message);
                    }
                } catch (error) {
                    this.pushError(transaction, "ERR_UNKNOWN", error.message);
                }
            }
        }
    }

    private async validateTransaction(transaction: Interfaces.ITransactionData): Promise<boolean> {
        try {
            // @TODO: this leaks private members, refactor this
            return Handlers.Registry.get(transaction.type, transaction.typeGroup).canEnterTransactionPool(
                transaction,
                this.pool,
                this,
            );
        } catch (error) {
            if (error instanceof Errors.InvalidTransactionTypeError) {
                this.pushError(
                    transaction,
                    "ERR_UNSUPPORTED",
                    `Invalidating transaction of unsupported type '${Enums.TransactionType[transaction.type]}'`,
                );
            } else {
                this.pushError(transaction, "ERR_UNKNOWN", error.message);
            }
        }

        return false;
    }

    private async addTransactionsToPool(): Promise<void> {
        const { notAdded }: ITransactionsProcessed = await this.pool.addTransactions(Array.from(this.accept.values()));

        for (const item of notAdded) {
            this.accept.delete(item.transaction.id);

            if (item.type !== "ERR_POOL_FULL") {
                this.broadcast.delete(item.transaction.id);
            }

            this.pushError(item.transaction.data, item.type, item.message);
        }
    }

    private printStats(): void {
        const stats: string = ["accept", "broadcast", "excess", "invalid"]
            .map(prop => `${prop}: ${this[prop] instanceof Array ? this[prop].length : this[prop].size}`)
            .join(" ");

        app.resolvePlugin<Logger.ILogger>("logger").info(
            `Received ${pluralize("transaction", this.transactions.length, true)} (${stats}).`,
        );
    }
}
