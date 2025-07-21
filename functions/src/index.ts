/**
 */
import * as functions from "firebase-functions";
import {onValueWritten} from "firebase-functions/v2/database";
import {BigQuery} from "@google-cloud/bigquery";
import * as admin from "firebase-admin";

const BQ_DATASET_ID = "sensor_data";
const BQ_WS_TABLE_ID = "wind_speed_obsv";

// Initialize the Firebase Admin SDK to access Firestore
admin.initializeApp();
const bigquery = new BigQuery();

/**
 * This function triggers when data is written (created or updated)
 * in the Realtime Database at the path /sensors/{sensorId}.
 * It then saves this data as a historical record in Firestore.
 */
export const saveHistoricalData = onValueWritten("/sensors", async (event) => {
  const sensorValue: number = event.data.after.val().wind_speed;
  const timestampString: string = event.data.after.val().timestamp;
  const sensorId = "anemometer_01";
  // If there's no data (e.g., the sensor node was deleted), exit the function.
  if (sensorValue === null) {
    functions.logger.log("Data was deleted, no historical record to save.");
    return;
  }

  // --- Conversion and Validation Logic ---
  // 1. Create a JavaScript Date object from the sensor's string.
  const dateObject: Date = new Date(timestampString);

  // 2. VERY IMPORTANT: Validate the date object.
  //    If the string is invalid, new Date() creates an "Invalid Date" object.
  if (isNaN(dateObject.getTime())) {
    functions.logger.error(
      `Invalid timestamp string received: "${timestampString}"`
    );
    return; // Exit the function
  }

  // 2. BigQuery record (row)
  // Note: BigQuery's TIMESTAMP type accepts the JS Date object directly.
  const bigQueryRow = {
    sensor_id: sensorId,
    observation: sensorValue,
    timestamp: dateObject,
  };
  try {
    await bigquery
      .dataset(BQ_DATASET_ID)
      .table(BQ_WS_TABLE_ID)
      .insert([bigQueryRow]);
    functions.logger.log("Successfully saved to BigQuery");
  } catch (error) {
    functions.logger.error("Error saving data:", error);
  }
});
