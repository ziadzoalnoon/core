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
import { PoolBroker } from './worker/pool-broker';
import { BrokerToWorker, IFinishedTransactionJobResult, IPendingTransactionJobResult } from './worker/types';
import { pushError } from './worker/utils';

export class Processor {
    private pendingTickets: Map<string, boolean> = new Map();
    private processedTickets: Map<string, IFinishedTransactionJobResult> = new Map();

    private readonly poolBroker: PoolBroker;
    private readonly queue: async.AsyncQueue<{ job: IPendingTransactionJobResult }>;

    public constructor(private readonly pool: TransactionPool.IConnection, private readonly walletManager: WalletManager) {
        this.poolBroker = new PoolBroker((job: IPendingTransactionJobResult) => this.queue.push({ job }));

        this.queue = async.queue(({ job }: { job: IPendingTransactionJobResult }, cb) => {
            const { ticketId, validTransactions, } = job;

            app.resolvePlugin<Logger.ILogger>("logger").debug(
                `Received ticket ${ticketId} with ${validTransactions.length} valid transactions from worker.`,
            );

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

    public getPendingTickets(): string[] {
        return [...this.pendingTickets.keys()];
    }

    public getProcessedTickets(): IFinishedTransactionJobResult[] {
        return [...this.processedTickets.values()];
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

            eligibleTransactions.push(transaction);
        }

        const ticketId: string = (await this.poolBroker.sendToWorker(BrokerToWorker.CreateJob, {
            transactions: eligibleTransactions,
            senderWallets,
        })).data;

        this.pendingTickets.set(ticketId, true);

        return ticketId;
    }

    private async finishTransactionJob(pendingJob: IPendingTransactionJobResult, cb: any): Promise<void> {
        pendingJob.accept = {};
        pendingJob.broadcast = {};

        const acceptedTransactions: Interfaces.ITransaction[] = await this.performWalletChecks(pendingJob);

        await this.removeForgedTransactions(pendingJob);
        await this.addToTransactionPool(acceptedTransactions, pendingJob);

        this.writeResult(pendingJob, acceptedTransactions);

        return cb();
    }

    private async preWorkerChecks(transaction: Interfaces.ITransactionData): Promise<boolean> {
        try {

            if (await this.pool.has(transaction.id)) {
                return false;
            }

            //  if (await this.pool.hasExceededMaxTransactions(transaction.senderPublicKey)) {
            //      return false;
            //  }

            const handler: Handlers.TransactionHandler = Handlers.Registry.get(transaction.type, transaction.typeGroup);

            if (!(await handler.canEnterTransactionPool(transaction, this.pool, undefined))) {
                return false;
            }

            return true;

        } catch {
            return false;
        }
    }

    private async performWalletChecks(pendingJob: IPendingTransactionJobResult): Promise<Interfaces.ITransaction[]> {
        const { validTransactions } = pendingJob;
        const acceptedTransactions: Interfaces.ITransaction[] = [];
        for (const { buffer, id } of validTransactions) {
            try {
                const transaction: Interfaces.ITransaction = Transactions.TransactionFactory.fromBytesUnsafe(buffer, id);

                try {
                    await this.walletManager.throwIfCannotBeApplied(transaction);
                    const dynamicFee: IDynamicFeeMatch = dynamicFeeMatcher(transaction);
                    if (!dynamicFee.enterPool && !dynamicFee.broadcast) {
                        pushError(pendingJob, transaction.id, {
                            type: "ERR_LOW_FEE",
                            message: "The fee is too low to broadcast and accept the transaction",
                        });

                        continue;

                    }

                    if (dynamicFee.enterPool) {
                        pendingJob.accept[transaction.id] = true;
                    }

                    if (dynamicFee.broadcast) {
                        pendingJob.broadcast[transaction.id] = true;
                    }

                    acceptedTransactions.push(transaction);

                } catch (error) {
                    pushError(pendingJob, transaction.id, {
                        type: "ERR_APPLY",
                        message: error.message,
                    });
                }
            } catch (error) {
                pushError(pendingJob, id, {
                    type: "ERR_UNKNOWN",
                    message: error.message
                });
            }
        }

        return acceptedTransactions;
    }

    private async removeForgedTransactions(pendingJob: IPendingTransactionJobResult): Promise<void> {
        const forgedIdsSet: string[] = await app
            .resolvePlugin<Database.IDatabaseService>("database")
            .getForgedTransactionsIds([
                ...new Set(
                    [
                        ...Object.keys(pendingJob.accept),
                        ...Object.keys(pendingJob.broadcast)
                    ])
            ]);

        for (const id of forgedIdsSet) {
            pushError(pendingJob, id, {
                type: "ERR_FORGED", message: "Already forged."
            });

            delete pendingJob.accept[id];
            delete pendingJob.broadcast[id];

            const index: number = pendingJob.validTransactions.findIndex(transaction => transaction.id === id);
            assert(index !== -1);
            pendingJob.validTransactions.splice(index, 1);
        }
    }

    private async addToTransactionPool(transactions: Interfaces.ITransaction[], pendingJob: IPendingTransactionJobResult): Promise<void> {
        const { notAdded } = await this.pool.addTransactions(transactions.filter(({ id }) => pendingJob.accept[id]));

        for (const item of notAdded) {
            delete pendingJob.accept[item.transaction.id];

            if (item.type !== "ERR_POOL_FULL") {
                delete pendingJob.broadcast[item.transaction.id];
            }

            pushError(pendingJob, item.transaction.id, {
                type: item.type,
                message: item.message
            });
        }
    }

    private writeResult(pendingJob: IPendingTransactionJobResult, validTransactions: Interfaces.ITransaction[]): void {
        const jobResult: IFinishedTransactionJobResult = {
            ticketId: pendingJob.ticketId,
            accept: Object.keys(pendingJob.accept),
            broadcast: Object.keys(pendingJob.broadcast),
            invalid: Object.keys(pendingJob.invalid),
            excess: Object.keys(pendingJob.excess),
            errors: Object.keys(pendingJob.errors).length > 0 ? pendingJob.errors : undefined,
        }

        this.pendingTickets.delete(jobResult.ticketId);
        this.processedTickets.set(jobResult.ticketId, jobResult);

        this.printStats(jobResult, validTransactions);
    }

    private printStats(finishedJob: IFinishedTransactionJobResult, validTransactions: Interfaces.ITransaction[]): void {
        const total: number = validTransactions.length + finishedJob.excess.length + finishedJob.invalid.length;
        const stats: string = ["accept", "broadcast", "excess", "invalid"]
            .map(prop => `${prop}: ${finishedJob[prop].length}`)
            .join(" ");

        app.resolvePlugin<Logger.ILogger>("logger").info(
            `Received ${pluralize("transaction", total, true)} (${stats}).`,
        );
    }

}


/**
 * @TODO: this class has too many responsibilities at the moment.
 * Its sole responsibility should be to validate transactions and return them.
 */
// tslint:disable-next-line: max-classes-per-file
export class LegacyProcessor implements TransactionPool.IProcessor {
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
