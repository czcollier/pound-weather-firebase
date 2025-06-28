/**
 */
import {setGlobalOptions} from "firebase-functions";
import {defineInt, defineString} from "firebase-functions/params";
import {onValueWritten} from "firebase-functions/v2/database";
import * as logger from "firebase-functions/logger";
import {onInit} from "firebase-functions/v2/core";
import {Pool} from "pg";

const minInstancesConfig = defineInt("MIN_INSTANCES");
const maxInstancesConfig = defineInt("MAX_INSTANCES");
const dbHost = defineString("DB_HOST");
const dbUser = defineString("DB_USER");
const dbPass = defineString("DB_PASSWORD");
const dbName = defineString("DB_NAME");


setGlobalOptions({
  minInstances: minInstancesConfig,
  maxInstances: maxInstancesConfig,
});


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

  logger.log(newWindSpeed);

  const dbClient = await pool.connect();

  try {
    await dbClient.query(
      `insert into wind_speed (value, timestamp, location)
      values (${newWindSpeed}, NOW(), 'Boston')`);
  } catch (e) {
    logger.error(`ERROR writing to database: ${e}`);
  } finally {
    dbClient.release();
  }
});
