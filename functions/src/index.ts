/**
 */
import * as functions from "firebase-functions";
import {defineInt, defineString} from "firebase-functions/params";
import {onValueWritten} from "firebase-functions/v2/database";
import * as logger from "firebase-functions/logger";
import {onInit} from "firebase-functions/v2/core";
import {Pool} from "pg";
import {Timestamp} from "firebase-admin/firestore";
import * as admin from "firebase-admin";

const minInstancesConfig = defineInt("MIN_INSTANCES");
const maxInstancesConfig = defineInt("MAX_INSTANCES");
const dbHost = defineString("DB_HOST");
const dbUser = defineString("DB_USER");
const dbPass = defineString("DB_PASSWORD");
const dbName = defineString("DB_NAME");


functions.setGlobalOptions({
  minInstances: minInstancesConfig,
  maxInstances: maxInstancesConfig,
});

// Initialize the Firebase Admin SDK to access Firestore
admin.initializeApp();
// Get a reference to the Firestore database
const db = admin.firestore();

let pool: Pool = new Pool({});

export const init = onInit(async () => {
  pool = new Pool({
    host: dbHost.value(),
    database: dbName.value(),
    user: dbUser.value(),
    password: dbPass.value(),
    port: 5432, // Default PostgreSQL port
  });
});

export const onRtWrite = onValueWritten("/sensors", async (event) => {
  logger.log(event);

  const newWindSpeed = event.data.after.val().wind_speed;
  const newTimestamp = event.data.after.val().timestamp;
  const adjustedWindSpeed = newWindSpeed * 2.5;
  // logger.log(newWindSpeed);

  const dbClient = await pool.connect();

  try {
    await dbClient.query(
      `insert into wind_speed (value, timestamp, server_timestamp, location)
      values (${adjustedWindSpeed}, '${newTimestamp}', NOW(), 'Boston')`);
  } catch (e) {
    logger.error(`ERROR writing to database: ${e}`);
  } finally {
    dbClient.release();
  }
});

/**
 * This function triggers when data is written (created or updated)
 * in the Realtime Database at the path /sensors/{sensorId}.
 * It then saves this data as a historical record in Firestore.
 */
export const saveHistoricalData = onValueWritten("/sensors", async (event) => {
    //change: functions.Change<functions.database.DataSnapshot>,
    //context: functions.EventContext
  //): Promise<admin.firestore.DocumentReference | null> => {
    // Grab the new value from the Realtime Database trigger.
    // change.after.val() holds the data after the write event.
    const sensorValue = event.data.after.val().wind_speed;
    const timestampString = event.data.after.val().timestamp;
    const sensorId = "anemometer_01";
    // If there's no data (e.g., the sensor node was deleted), exit the function.
    if (sensorValue === null) {
      functions.logger.log("Data was deleted, no historical record to save.");
      return null;
    }

     // --- Conversion and Validation Logic ---
    // 1. Create a JavaScript Date object from the sensor's string.
    const dateObject = new Date(timestampString);

    // 2. VERY IMPORTANT: Validate the date object.
    //    If the string is invalid, new Date() creates an "Invalid Date" object.
    if (isNaN(dateObject.getTime())) {
      functions.logger.error(
        `Invalid timestamp string received: "${timestampString}"`
      );
      return null; // Exit the function
    }

    // 3. Convert the valid Date object to a Firestore Timestamp.
    const timestamp = Timestamp.fromDate(dateObject);
    // --- End of Conversion Logic ---


    // Get the sensorId from the wildcard in the database path.
    //const sensorId: string = context.params.sensorId;

    functions.logger.log(
      `New reading for ${sensorId}: ${sensorValue}. Saving to Firestore...`
    );

    // Create a new document in the 'historical_data' collection.
    try {
      const docRef = await db.collection("historical_data").add({
        sensorId: sensorId,
        value: sensorValue,
        timestamp: timestamp //admin.firestore.FieldValue.serverTimestamp(),
      });
      functions.logger.log(
        "Historical record saved successfully with ID:",
        docRef.id
      );
      return docRef;
    } catch (error) {
      functions.logger.error("Error saving historical data to Firestore:", error);
      return null;
    }
  });