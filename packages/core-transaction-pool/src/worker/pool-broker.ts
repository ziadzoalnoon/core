import { app } from "@arkecosystem/core-container";
import { ApplicationEvents } from "@arkecosystem/core-event-emitter";
import { EventEmitter } from "@arkecosystem/core-interfaces";
import { Interfaces, Managers } from "@arkecosystem/crypto";
import { Worker } from "worker_threads";
import { ProcessorV2 } from '../processor';
import { IMessageObject } from "./worker";

export enum BrokerToWorker {
    Initialize = "initialize",
    CreateJob = "createJob",
    BlockHeightUpdate = "blockHeightUpdate",
}

export enum WorkerToBroker {
    TicketId = "ticketId",
    TransactionJobResult = "transactionJobResult",
    Unknown = "unknown",
}

export class PoolBroker {
    private worker: Worker;

    public constructor(private readonly processor: ProcessorV2) {
        this.setupWorker();

        app.resolvePlugin<EventEmitter.EventEmitter>("event-emitter").on(
            ApplicationEvents.BlockApplied,
            (data: Interfaces.IBlockData) => {
                this.sendToWorker(BrokerToWorker.BlockHeightUpdate, data.height);
            },
        );
    }

    public async sendToWorker(type: BrokerToWorker, data: any): Promise<any> {
        console.log("Emitting to worker: " + type);
        return new Promise((resolve, reject) => {
            this.worker.postMessage({ type, data });

            this.worker.once("message", data => {
                resolve(data);
            });
            this.worker.once("error", error => reject(error));
        });
    }

    private setupWorker(): void {
        if (this.worker) {
            this.worker.terminate();
        }

        this.worker = new Worker("../core-transaction-pool/dist/worker/worker.js");

        const options: Record<string, any> = ({
            ...app.resolveOptions("transaction-pool"),
            networkName: Managers.configManager.get("network.name")
        });

        this.sendToWorker(BrokerToWorker.Initialize, options);

        this.worker.on("message", (message: IMessageObject<WorkerToBroker>) => {
            console.log("Worker message: " + message.type);

            switch (message.type) {
                case WorkerToBroker.TicketId: {
                    break;
                }
                case WorkerToBroker.TransactionJobResult: {
                    this.processor.addFinishedWorkerJobToQueue(message.data);
                    break;
                }
                default: {
                    //
                }
            }
        });
        this.worker.on("error", e => {
            console.log(e.stack);
        });
        this.worker.on("exit", code => {
            console.log("Worker exittng...");
            if (code !== 0) {
                console.log(code);
            }
        });
    }
}
