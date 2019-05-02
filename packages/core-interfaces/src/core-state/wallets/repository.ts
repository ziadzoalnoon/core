import { IWallet } from "./wallet";

export interface IWalletRepository {
    allByAddress(): IWallet[];

    allByPublicKey(): IWallet[];

    allByUsername(): IWallet[];

    findByAddress(address: string): IWallet;

    has(addressOrPublicKey: string): boolean;

    findByPublicKey(publicKey: string): IWallet;

    findByUsername(username: string): IWallet;

    forgetByAddress(address: string): void;

    forgetByPublicKey(publicKey: string): void;

    forgetByUsername(username: string): void;

    setByAddress(address: string, wallet: IWallet): void;

    setByPublicKey(publicKey: string, wallet: IWallet): void;

    setByUsername(username: string, wallet: IWallet): void;

    hasByAddress(address: string): boolean;

    hasByPublicKey(publicKey: string): boolean;

    hasByUsername(username: string): boolean;

    index(wallets: IWallet | IWallet[]): void;

    reset(): void;
}
