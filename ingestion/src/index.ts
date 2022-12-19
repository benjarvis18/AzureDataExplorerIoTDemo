import { F1TelemetryClient, constants } from "@racehub-io/f1-telemetry-client";

import { EventHubProducerClient } from "@azure/event-hubs";

import * as dotenv from "dotenv"

const { PACKETS } = constants;

/*
 *   'port' is optional, defaults to 20777
 *   'bigintEnabled' is optional, setting it to false makes the parser skip bigint values,
 *                   defaults to true
 *   'forwardAddresses' is optional, it's an array of Address objects to forward unparsed telemetry to. each address object is comprised of a port and an optional ip address
 *                   defaults to undefined
 *   'skipParsing' is optional, setting it to true will make the client not parse and emit content. You can consume telemetry data using forwardAddresses instead.
 *                   defaults to false
 */

const getEventHub = (eventType: string) => {
  switch (eventType) {
    case PACKETS.event:
      return "event";
    case PACKETS.motion:
      return "motion";
    case PACKETS.carSetups:
      return "car-setups";
    case PACKETS.lapData:
      return "lap-data";
    case PACKETS.session:
      return "session";
    case PACKETS.participants:
      return "game-telemetry";
    case PACKETS.carTelemetry:
      return "car-telemetry";
    case PACKETS.carStatus:
      return "car-status";
    case PACKETS.carDamage:
      return "car-damage";
    default:
      return "game-telemetry";
  }
};

const createProducerClient = (eventHubName: string) => {
  return new EventHubProducerClient(process.env.EVENT_HUB_CONNECTION_STRING ?? "", eventHubName);
}

const producerClients = new Map<string, EventHubProducerClient>();

const getOrCreateProducerClient = (eventHubName: string): EventHubProducerClient => {
  const producerClient = producerClients.get(eventHubName)
  
  if (producerClient) {
    return producerClient
  }
  else {
    const newProducerClient = createProducerClient(eventHubName)
    producerClients.set(eventHubName, newProducerClient)

    return newProducerClient
  }
}

const writeToEventHub = async (eventType: string, data: any) => {
  console.log("Processing " + eventType)
  console.log(data)

  const producerClient = getOrCreateProducerClient(getEventHub(eventType))

  const eventDataBatch = await producerClient.createBatch();
  const wasAdded = eventDataBatch.tryAdd(data);
  
  if (!wasAdded) {
    console.error("Couldn't add event to batch")
  }

  await producerClient.sendBatch(eventDataBatch);
}

dotenv.config({ path: __dirname + "/.env" });

if (!process.env.EVENT_HUB_CONNECTION_STRING) {
  console.error("No Event Hub connection string specified")
  process.exit(-1)
}

const client = new F1TelemetryClient({ port: 20777 });
client.on(PACKETS.event, async (data) => await writeToEventHub(PACKETS.event, data));
client.on(PACKETS.motion, async (data) => await writeToEventHub(PACKETS.motion, data));
client.on(PACKETS.carSetups, async (data) => await writeToEventHub(PACKETS.carSetups, data));
client.on(PACKETS.lapData, async (data) => await writeToEventHub(PACKETS.lapData, data));
client.on(PACKETS.session, async (data) => await writeToEventHub(PACKETS.session, data));
client.on(PACKETS.participants, async (data) => await writeToEventHub(PACKETS.participants, data));
client.on(PACKETS.carTelemetry, async (data) => await writeToEventHub(PACKETS.carTelemetry, data));
client.on(PACKETS.carStatus, async (data) => await writeToEventHub(PACKETS.carStatus, data));
client.on(PACKETS.finalClassification, async (data) => await writeToEventHub(PACKETS.finalClassification, data));
client.on(PACKETS.lobbyInfo, async (data) => await writeToEventHub(PACKETS.lobbyInfo, data));
client.on(PACKETS.carDamage, async (data) => await writeToEventHub(PACKETS.carDamage, data));
client.on(PACKETS.sessionHistory, async (data) => await writeToEventHub(PACKETS.sessionHistory, data));

// to start listening:
client.start();
