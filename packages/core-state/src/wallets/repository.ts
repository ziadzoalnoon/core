import { State } from "@arkecosystem/core-interfaces";
import { Identities } from "@arkecosystem/crypto";
import { Wallet } from "./wallet";

// @TODO: bind an instance of this to the state service
export class Repository implements State.IWalletRepository {
    protected readonly byAddress: Map<string, State.IWallet> = new Map<string, State.IWallet>();
    protected readonly byPublicKey: Map<string, State.IWallet> = new Map<string, State.IWallet>();
    protected readonly byUsername: Map<string, State.IWallet> = new Map<string, State.IWallet>();

    public allByAddress(): State.IWallet[] {
        return [...this.byAddress.values()];
    }

    public allByPublicKey(): State.IWallet[] {
        return [...this.byPublicKey.values()];
    }

    public allByUsername(): State.IWallet[] {
        return [...this.byUsername.values()];
    }

    public findByAddress(address: string): State.IWallet {
        if (address && !this.hasByAddress(address)) {
            this.byAddress.set(address, new Wallet(address));
        }

        return this.byAddress.get(address);
    }

    public findByPublicKey(publicKey: string): State.IWallet {
        if (publicKey && !this.hasByPublicKey(publicKey)) {
            const wallet: State.IWallet = this.findByAddress(Identities.Address.fromPublicKey(publicKey));
            wallet.publicKey = publicKey;

            this.byPublicKey.set(publicKey, wallet);
        }

        return this.byPublicKey.get(publicKey);
    }

    public findByUsername(username: string): State.IWallet {
        return this.byUsername.get(username);
    }

    public setByAddress(address: string, wallet: Wallet): void {
        if (address && wallet) {
            this.byAddress.set(address, wallet);
        }
    }

    public setByPublicKey(publicKey: string, wallet: Wallet): void {
        if (publicKey && wallet) {
            this.byPublicKey.set(publicKey, wallet);
        }
    }

    public setByUsername(username: string, wallet: Wallet): void {
        if (username && wallet) {
            this.byUsername.set(username, wallet);
        }
    }

    public has(addressOrPublicKey: string): boolean {
        return this.hasByAddress(addressOrPublicKey) || this.hasByPublicKey(addressOrPublicKey);
    }

    public hasByAddress(address: string): boolean {
        return this.byAddress.has(address);
    }

    public hasByPublicKey(publicKey: string): boolean {
        return this.byPublicKey.has(publicKey);
    }

    public hasByUsername(username: string): boolean {
        return this.byUsername.has(username);
    }

    public forgetByAddress(address: string): void {
        this.byAddress.delete(address);
    }

    public forgetByPublicKey(publicKey: string): void {
        this.byPublicKey.delete(publicKey);
    }

    public forgetByUsername(username: string): void {
        this.byUsername.delete(username);
    }

    public index(wallets: State.IWallet | State.IWallet[]): void {
        if (!Array.isArray(wallets)) {
            wallets = [wallets];
        }

        for (const wallet of wallets) {
            if (wallet.address) {
                this.byAddress.set(wallet.address, wallet);
            }

            if (wallet.publicKey) {
                this.byPublicKey.set(wallet.publicKey, wallet);
            }

            if (wallet.username) {
                this.byUsername.set(wallet.username, wallet);
            }
        }
    }

    public reset(): void {
        this.byAddress.clear();
        this.byPublicKey.clear();
        this.byUsername.clear();
    }
}
