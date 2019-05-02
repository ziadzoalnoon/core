import { app } from "@arkecosystem/core-container";
import { Logger, Shared, State } from "@arkecosystem/core-interfaces";
import { Handlers } from "@arkecosystem/core-transactions";
import { Enums, Identities, Interfaces, Utils } from "@arkecosystem/crypto";
import pluralize from "pluralize";
import { Repository } from "./repository";
import { Wallet } from "./wallet";

/**
 * @TODO
 *
 * - rename this
 * - bind this to the state service
 */
export class WalletManager implements State.IWalletManager {
    public constructor(protected readonly repository: State.IWalletRepository) {}

    /**
     * @TODO
     *
     * - remove this after all required restructuring has been done
     */
    public getRepository(): State.IWalletRepository {
        return this.repository;
    }

    public cloneDelegateWallets(): State.IWalletManager {
        const repository: State.IWalletRepository = new Repository();
        repository.index(this.repository.allByUsername());

        return new WalletManager(repository);
    }

    public loadActiveDelegateList(roundInfo: Shared.IRoundInfo): State.IDelegateWallet[] {
        const { maxDelegates }: Shared.IRoundInfo = roundInfo;
        const delegatesWallets: State.IWallet[] = this.repository.allByUsername();

        if (delegatesWallets.length < maxDelegates) {
            throw new Error(
                `Expected to find ${maxDelegates} delegates but only found ${
                    delegatesWallets.length
                }. This indicates an issue with the genesis block & delegates.`,
            );
        }

        const delegates: State.IWallet[] = this.buildDelegateRanking(delegatesWallets, roundInfo);

        app.resolvePlugin<Logger.ILogger>("logger").debug(
            `Loaded ${delegates.length} active ${pluralize("delegate", delegates.length)}`,
        );

        return delegates as State.IDelegateWallet[];
    }

    // Only called during integrity verification on boot.
    public buildVoteBalances(): void {
        for (const voter of this.repository.allByPublicKey()) {
            if (voter.vote) {
                const delegate: State.IWallet = this.repository.findByPublicKey(voter.vote);
                delegate.voteBalance = delegate.voteBalance.plus(voter.balance);
            }
        }
    }

    public canBePurged(wallet: State.IWallet): boolean {
        return wallet.balance.isZero() && !wallet.secondPublicKey && !wallet.multisignature && !wallet.username;
    }

    public buildDelegateRanking(delegates: State.IWallet[], roundInfo?: Shared.IRoundInfo): State.IDelegateWallet[] {
        // @TODO: add type
        const equalVotesMap = new Map();
        let delegateWallets = delegates
            .sort((a, b) => {
                const diff = b.voteBalance.comparedTo(a.voteBalance);

                if (diff === 0) {
                    if (!equalVotesMap.has(a.voteBalance.toFixed())) {
                        equalVotesMap.set(a.voteBalance.toFixed(), new Set());
                    }

                    const set = equalVotesMap.get(a.voteBalance.toFixed());
                    set.add(a);
                    set.add(b);

                    if (a.publicKey === b.publicKey) {
                        throw new Error(
                            `The balance and public key of both delegates are identical! Delegate "${
                                a.username
                            }" appears twice in the list.`,
                        );
                    }

                    return a.publicKey.localeCompare(b.publicKey, "en");
                }

                return diff;
            })
            .map((delegate, i) => {
                const rate = i + 1;

                const wallet: State.IWallet = this.repository.findByUsername(delegate.username);
                wallet.rate = rate;
                this.repository.index(wallet);

                return { round: roundInfo ? roundInfo.round : 0, ...delegate, rate };
            });

        if (roundInfo) {
            delegateWallets = delegateWallets.slice(0, roundInfo.maxDelegates);

            for (const [voteBalance, set] of equalVotesMap.entries()) {
                const values: any[] = Array.from(set.values());
                if (delegateWallets.includes(values[0])) {
                    app.resolvePlugin<Logger.ILogger>("logger").warn(
                        `Delegates ${JSON.stringify(
                            values.map(v => `${v.username} (${v.publicKey})`),
                            undefined,
                            4,
                        )} have a matching vote balance of ${Utils.formatSatoshi(voteBalance)}`,
                    );
                }
            }
        }

        return delegateWallets;
    }

    /**
     * Updates the vote balances of the respective delegates of sender and recipient.
     * If the transaction is not a vote...
     *    1. fee + amount is removed from the sender's delegate vote balance
     *    2. amount is added to the recipient's delegate vote balance
     *
     * in case of a vote...
     *    1. the full sender balance is added to the sender's delegate vote balance
     *
     * If revert is set to true, the operations are reversed (plus -> minus, minus -> plus).
     */
    public updateVoteBalances(
        sender: State.IWallet,
        recipient: State.IWallet,
        transaction: Interfaces.ITransactionData,
        revert?: boolean,
    ): void {
        // TODO: multipayment?
        if (transaction.type !== Enums.TransactionTypes.Vote) {
            // Update vote balance of the sender's delegate
            if (sender.vote) {
                const delegate: State.IWallet = this.repository.findByPublicKey(sender.vote);
                const total: Utils.BigNumber = transaction.amount.plus(transaction.fee);
                delegate.voteBalance = revert ? delegate.voteBalance.plus(total) : delegate.voteBalance.minus(total);
            }

            // Update vote balance of recipient's delegate
            if (recipient && recipient.vote) {
                const delegate: State.IWallet = this.repository.findByPublicKey(recipient.vote);
                delegate.voteBalance = revert
                    ? delegate.voteBalance.minus(transaction.amount)
                    : delegate.voteBalance.plus(transaction.amount);
            }
        } else {
            const vote: string = transaction.asset.votes[0];
            const delegate: State.IWallet = this.repository.findByPublicKey(vote.substr(1));

            if (vote.startsWith("+")) {
                delegate.voteBalance = revert
                    ? delegate.voteBalance.minus(sender.balance.minus(transaction.fee))
                    : delegate.voteBalance.plus(sender.balance);
            } else {
                delegate.voteBalance = revert
                    ? delegate.voteBalance.plus(sender.balance)
                    : delegate.voteBalance.minus(sender.balance.plus(transaction.fee));
            }
        }
    }

    /**
     * @TODO
     *
     * This should be moved out because it is only called by `core-database`.
     * It is currently placed here because `calcPreviousActiveDelegates` in `core-database`
     * uses a temporary wallet manager to revert blocks.
     */
    public applyBlock(block: Interfaces.IBlock): void {
        const generatorPublicKey: string = block.data.generatorPublicKey;

        let delegate: State.IWallet = this.repository.findByPublicKey(block.data.generatorPublicKey);

        if (!delegate) {
            const generator: string = Identities.Address.fromPublicKey(generatorPublicKey);

            if (block.data.height === 1) {
                delegate = new Wallet(generator);
                delegate.publicKey = generatorPublicKey;

                this.repository.index(delegate);
            } else {
                app.resolvePlugin<Logger.ILogger>("logger").debug(
                    `Delegate by address: ${this.repository.findByAddress(generator)}`,
                );

                if (this.repository.hasByAddress(generator)) {
                    app.resolvePlugin<Logger.ILogger>("logger").info("This look like a bug, please report");
                }

                throw new Error(`Could not find delegate with publicKey ${generatorPublicKey}`);
            }
        }

        const appliedTransactions: Interfaces.ITransaction[] = [];

        try {
            block.transactions.forEach(transaction => {
                this.applyTransaction(transaction);
                appliedTransactions.push(transaction);
            });

            const applied: boolean = delegate.applyBlock(block.data);

            // If the block has been applied to the delegate, the balance is increased
            // by reward + totalFee. In which case the vote balance of the
            // delegate's delegate has to be updated.
            if (applied && delegate.vote) {
                const increase: Utils.BigNumber = block.data.reward.plus(block.data.totalFee);
                const votedDelegate: State.IWallet = this.repository.findByPublicKey(delegate.vote);
                votedDelegate.voteBalance = votedDelegate.voteBalance.plus(increase);
            }
        } catch (error) {
            app.resolvePlugin<Logger.ILogger>("logger").error(
                "Failed to apply all transactions in block - reverting previous transactions",
            );

            // Revert the applied transactions from last to first
            appliedTransactions
                .reverse()
                .forEach((transaction: Interfaces.ITransaction) => this.revertTransaction(transaction));

            // for (let i = appliedTransactions.length - 1; i >= 0; i--) {
            //     this.revertTransaction(appliedTransactions[i]);
            // }
            // TODO: should revert the delegate applyBlock ?
            // TBC: whatever situation `delegate.applyBlock(block.data)` is never applied

            throw error;
        }
    }

    public revertBlock(block: Interfaces.IBlock): void {
        const delegate: State.IWallet = this.repository.findByPublicKey(block.data.generatorPublicKey);

        if (!delegate) {
            app.forceExit(`Failed to lookup generator '${block.data.generatorPublicKey}' of block '${block.data.id}'.`);
        }

        const revertedTransactions: Interfaces.ITransaction[] = [];

        try {
            // Revert the transactions from last to first
            for (let i = block.transactions.length - 1; i >= 0; i--) {
                const transaction: Interfaces.ITransaction = block.transactions[i];
                this.revertTransaction(transaction);
                revertedTransactions.push(transaction);
            }

            const reverted: boolean = delegate.revertBlock(block.data);

            // If the block has been reverted, the balance is decreased
            // by reward + totalFee. In which case the vote balance of the
            // delegate's delegate has to be updated.
            if (reverted && delegate.vote) {
                const decrease: Utils.BigNumber = block.data.reward.plus(block.data.totalFee);
                const votedDelegate: State.IWallet = this.repository.findByPublicKey(delegate.vote);
                votedDelegate.voteBalance = votedDelegate.voteBalance.minus(decrease);
            }
        } catch (error) {
            app.resolvePlugin<Logger.ILogger>("logger").error(error.stack);

            revertedTransactions
                .reverse()
                .forEach((transaction: Interfaces.ITransaction) => this.applyTransaction(transaction));

            throw error;
        }
    }

    public applyTransaction(transaction: Interfaces.ITransaction): void {
        const { data } = transaction;
        const { type, recipientId, senderPublicKey } = data;

        const transactionHandler: Handlers.TransactionHandler = Handlers.Registry.get(transaction.type);
        const sender: State.IWallet = this.repository.findByPublicKey(senderPublicKey);
        const recipient: State.IWallet = this.repository.findByAddress(recipientId);

        // TODO: can/should be removed?
        if (type === Enums.TransactionTypes.SecondSignature) {
            data.recipientId = "";
        }

        // handle exceptions / verify that we can apply the transaction to the sender
        if (Utils.isException(data)) {
            app.resolvePlugin<Logger.ILogger>("logger").warn(
                `Transaction ${data.id} forcibly applied because it has been added as an exception.`,
            );
        } else {
            try {
                transactionHandler.canBeApplied(transaction, sender, this);
            } catch (error) {
                app.resolvePlugin<Logger.ILogger>("logger").error(
                    `Can't apply transaction id:${data.id} from sender:${sender.address} due to ${error.message}`,
                );
                app.resolvePlugin<Logger.ILogger>("logger").debug(
                    `Audit: ${JSON.stringify(sender.auditApply(data), undefined, 2)}`,
                );
                throw new Error(`Can't apply transaction ${data.id}`);
            }
        }

        transactionHandler.apply(transaction, this);
        this.updateVoteBalances(sender, recipient, data);
    }

    public revertTransaction(transaction: Interfaces.ITransaction): void {
        const { data } = transaction;

        const transactionHandler: Handlers.TransactionHandler = Handlers.Registry.get(transaction.type);
        const sender: State.IWallet = this.repository.findByPublicKey(data.senderPublicKey);
        const recipient: State.IWallet = this.repository.findByAddress(data.recipientId);

        transactionHandler.revert(transaction, this);

        // Revert vote balance updates
        this.updateVoteBalances(sender, recipient, data, true);
    }
}
