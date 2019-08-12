import { app } from "@arkecosystem/core-container";
import { Database, Logger, P2P, State, TransactionPool } from "@arkecosystem/core-interfaces";
import { Interfaces, Transactions } from "@arkecosystem/crypto";
import assert from "assert";
import debounceFn, { DebouncedFunction } from "debounce-fn";
import delay from "delay";
import chunk from "lodash.chunk";
import PQueue from "p-queue";
import pluralize from "pluralize";
import uuidv4 from "uuid/v4";
import { dynamicFeeMatcher } from "./dynamic-fee";
import { IDynamicFeeMatch, ITransactionsCached } from "./interfaces";
import { WalletManager } from "./wallet-manager";
import { PoolBroker } from './worker/pool-broker';
import { IPendingTransactionJobResult } from './worker/types';
import { pushError } from './worker/utils';

export class Processor {

    public static async make(pool: TransactionPool.IConnection, walletManager: WalletManager): Promise<Processor> {
        return new Processor(pool, walletManager).init();
    }

    private readonly pendingTickets: Map<string, boolean> = new Map();
    private readonly processedTickets: Map<string, TransactionPool.IFinishedTransactionJobResult> = new Map();

    private readonly broadcastDebouncer: DebouncedFunction<any, any>;
    private readonly readyForBroadcast: Interfaces.ITransaction[] = [];

    private readonly poolBroker: PoolBroker;
    private readonly postWorkerQueue: PQueue = new PQueue({ concurrency: 1 });

    private constructor(private readonly pool: TransactionPool.IConnection, private readonly walletManager: WalletManager) {
        this.poolBroker = new PoolBroker((job: IPendingTransactionJobResult) => {
            this.postWorkerQueue.add(async () => this.postWorkerQueueTask(job))
        });

        this.broadcastDebouncer = debounceFn(async () => {
            const batchSize: number = app.resolveOptions("transaction-pool").maxTransactionsPerRequest;
            const transactions: Interfaces.ITransaction[] = this.readyForBroadcast.splice(0, batchSize * 3)

            for (const batch of chunk(transactions, batchSize)) {
                app.resolvePlugin<P2P.IPeerService>("p2p")
                    .getMonitor()
                    .broadcastTransactions(batch);
            }

            await this.postWorkerQueue.onIdle();
            this.broadcastDebouncer();
        }, { wait: 300, immediate: true })

        this.postWorkerQueue.on('active', () => {
            //  console.log(`PostSize: ${this.postWorkerQueue.size}  Pending: ${this.postWorkerQueue.pending}`);
        });
    }

    public getPendingTickets(): string[] {
        return [...this.pendingTickets.keys()];
    }

    public getProcessedTickets(): TransactionPool.IFinishedTransactionJobResult[] {
        return [...this.processedTickets.values()];
    }

    public hasPendingTicket(ticketId: string): boolean {
        return this.pendingTickets.has(ticketId);
    }

    public getProcessedTicket(ticketId: string): TransactionPool.IFinishedTransactionJobResult | undefined {
        return this.processedTickets.get(ticketId);
    }

    public async enqueueTransactions(transactions: Interfaces.ITransactionData[]): Promise<string> {
        await delay(1);

        const senderWallets: Record<string, State.IWallet> = {};
        const { added }: ITransactionsCached = app
            .resolvePlugin<State.IStateService>("state")
            .getStore()
            .cacheTransactions(transactions);

        for (const transaction of added) {
            // TODO: cold wallet check
            const senderWallet: State.IWallet = this.walletManager.findByPublicKey(transaction.senderPublicKey);
            senderWallets[transaction.senderPublicKey] = senderWallet;
        }

        let ticketId: string = "";
        if (added.length > 0) {
            ticketId = uuidv4();
            this.pendingTickets.set(ticketId, true);

            setImmediate(() => {
                this.poolBroker.createJob({
                    senderWallets,
                    transactions,
                    ticketId
                });
            });
        }

        return ticketId;
    }

    private async postWorkerQueueTask(job: IPendingTransactionJobResult): Promise<void> {
        try {
            await delay(10) // TODO: throttle
            return this.finishTransactionJob(job);
        } catch (error) {
            console.log(error.stack);
        }
    }

    private async init(): Promise<this> {
        await this.poolBroker.init();
        return this;
    }

    private async finishTransactionJob(pendingJob: IPendingTransactionJobResult): Promise<void> {
        pendingJob.accept = {};
        pendingJob.broadcast = {};

        const acceptedTransactions: Interfaces.ITransaction[] = await this.performWalletChecks(pendingJob);

        await this.removeForgedTransactions(pendingJob);
        await this.addToTransactionPool(acceptedTransactions, pendingJob);

        const broadcastBatch: Interfaces.ITransaction[] = this.writeResult(pendingJob, acceptedTransactions);
        this.readyForBroadcast.push(...broadcastBatch);

        this.broadcastDebouncer();
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
                        pendingJob.accept[transaction.id] = transaction;
                    }

                    if (dynamicFee.broadcast) {
                        pendingJob.broadcast[transaction.id] = transaction;
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
        const { notAdded } = await this.pool.addTransactions(transactions.filter(({ id }) => !!pendingJob.accept[id]));

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

    private writeResult(pendingJob: IPendingTransactionJobResult, validTransactions: Interfaces.ITransaction[]): Interfaces.ITransaction[] {
        const jobResult: TransactionPool.IFinishedTransactionJobResult = {
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

        return Object.values(pendingJob.broadcast);
    }

    private printStats(finishedJob: TransactionPool.IFinishedTransactionJobResult, validTransactions: Interfaces.ITransaction[]): void {
        const total: number = validTransactions.length + finishedJob.excess.length + finishedJob.invalid.length;
        const stats: string = ["accept", "broadcast", "excess", "invalid"]
            .map(prop => `${prop}: ${finishedJob[prop].length}`)
            .join(" ");

        app.resolvePlugin<Logger.ILogger>("logger").info(
            `Received ${pluralize("transaction", total, true)} (${stats}).`,
        );
    }

}
