import { app } from "@arkecosystem/core-container";
import { Database, Logger, State } from "@arkecosystem/core-interfaces";
import { Wallets } from "@arkecosystem/core-state";
import { Handlers } from "@arkecosystem/core-transactions";
import { Identities, Interfaces, Utils } from "@arkecosystem/crypto";

export class WalletManager extends Wallets.WalletManager {
    private readonly databaseService: Database.IDatabaseService = app.resolvePlugin<Database.IDatabaseService>(
        "database",
    );

    public findByAddress(address: string): State.IWallet {
        if (address && !this.getRepository().hasByAddress(address)) {
            this.getRepository().index(
                Object.assign(
                    new Wallets.Wallet(address),
                    this.databaseService.walletManager.getRepository().findByAddress(address),
                ),
            );
        }

        return this.getRepository().findByAddress(address);
    }

    public forget(publicKey: string): void {
        this.getRepository().forgetByPublicKey(publicKey);
        this.getRepository().forgetByAddress(Identities.Address.fromPublicKey(publicKey));
    }

    public throwIfApplyingFails(transaction: Interfaces.ITransaction): void {
        // Edge case if sender is unknown and has no balance.
        // NOTE: Check is performed against the database wallet manager.
        const { senderPublicKey } = transaction.data;
        if (!this.databaseService.walletManager.getRepository().has(senderPublicKey)) {
            const senderAddress: string = Identities.Address.fromPublicKey(senderPublicKey);

            if (
                this.databaseService.walletManager
                    .getRepository()
                    .findByAddress(senderAddress)
                    .balance.isZero()
            ) {
                const message: string = "Cold wallet is not allowed to send until receiving transaction is confirmed.";

                app.resolvePlugin<Logger.ILogger>("logger").error(message);

                throw new Error(JSON.stringify([message]));
            }
        }

        if (Utils.isException(transaction.data)) {
            app.resolvePlugin<Logger.ILogger>("logger").warn(
                `Transaction forcibly applied because it has been added as an exception: ${transaction.id}`,
            );
        } else {
            const sender: State.IWallet = this.getRepository().findByPublicKey(senderPublicKey);

            try {
                Handlers.Registry.get(transaction.type).canBeApplied(
                    transaction,
                    sender,
                    this.databaseService.walletManager,
                );
            } catch (error) {
                app.resolvePlugin<Logger.ILogger>("logger").error(
                    `[PoolWalletManager] Can't apply transaction ${transaction.id} from ${
                        sender.address
                    } due to ${JSON.stringify(error.message)}`,
                );

                throw new Error(JSON.stringify([error.message]));
            }
        }
    }

    public revertTransactionForSender(transaction: Interfaces.ITransaction): void {
        Handlers.Registry.get(transaction.type).revertForSenderInPool(transaction, this);
    }
}
