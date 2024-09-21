import express from "express";
import fetch from "node-fetch";

import { Worker } from "worker_threads";
const app = express();
const PORT = 3000;
// const worker = new Worker("./webWorker.js");
const apiToken =
  "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICI3Ny1NUVdFRTNHZE5adGlsWU5IYmpsa2dVSkpaWUJWVmN1UmFZdHl5ejFjIn0.eyJleHAiOjE3MjE4NjU3NzYsImlhdCI6MTcyMTgyOTc3NiwianRpIjoiYWZmOWU3MDItMTMzMS00NjQ3LWI1YmItZTQxNzczMDNhOTU0IiwiaXNzIjoiaHR0cDovL2tleWNsb2FrLmtleWNsb2FrLnN2Yy5jbHVzdGVyLmxvY2FsOjgwODAvcmVhbG1zL21hc3RlciIsImF1ZCI6ImFjY291bnQiLCJzdWIiOiI2MGFiN2ExZi05YTY3LTRiNTEtYmRjZC05MWU4YTY3ZGE4NjIiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJIT0xBQ1JBQ1kiLCJzZXNzaW9uX3N0YXRlIjoiZmM2YmQ4MjktNGRmMS00YzQxLWI2NjYtMDgxYTU5MGVhMDY4IiwibmFtZSI6Ik1vYml1cyBjbGlua3NAbW9iaXVzZHRhYXMuYWkiLCJnaXZlbl9uYW1lIjoiTW9iaXVzIiwiZmFtaWx5X25hbWUiOiJjbGlua3NAbW9iaXVzZHRhYXMuYWkiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJwYXNzd29yZF90ZW5hbnRfY2xpbmtzQG1vYml1c2R0YWFzLmFpIiwiZW1haWwiOiJwYXNzd29yZF90ZW5hbnRfY2xpbmtzQG1vYml1c2R0YWFzLmFpIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImFjciI6IjEiLCJhbGxvd2VkLW9yaWdpbnMiOlsiLyoiXSwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbImRlZmF1bHQtcm9sZXMtbWFzdGVyIiwib2ZmbGluZV9hY2Nlc3MiLCJ1bWFfYXV0aG9yaXphdGlvbiJdfSwicmVzb3VyY2VfYWNjZXNzIjp7IkhPTEFDUkFDWSI6eyJyb2xlcyI6WyJIT0xBQ1JBQ1lfVVNFUiJdfSwiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwic2NvcGUiOiJwcm9maWxlIGVtYWlsIiwic2lkIjoiZmM2YmQ4MjktNGRmMS00YzQxLWI2NjYtMDgxYTU5MGVhMDY4IiwidGVuYW50SWQiOiI2MGFiN2ExZi05YTY3LTRiNTEtYmRjZC05MWU4YTY3ZGE4NjIifQ==.Ppr4yJc9GacrY-CxjtwERyTDbHl81CldyxIqfZ2fjHNPcNrVtSfiYqgCYSeKsOJ07MsDNlpEnvdbUp2Uo3cBWXSlpsSS2wNuJKHUvVMsHl5LgzQYviFT4i8AtUVlSr67PMhYNiVr2eA5ewg50-GqV-DOcFYvcrX0-Nkq9V0Bis-EInAVb7K3ElQzJGa1kDpps9eAvoypKCp9_BkoRc-rVhai0zXCTjh0VKDySqjZxcDf7I_W29lzMwi_Af_QzUgEep_4TCh5ZjCrZ8xAePvA5Btslf8mLOACwaaWRzAJpOKO97VaSEf6Z9HXA4sDhKiZ9dsVTlWhauoNw7wcqz-0lQ";

const headers = {
  "Content-Type": "application/json",
  Authorization: `Bearer ${apiToken}`,
};



async function postDisastersData(schemaId, data) {
  const response = await fetch(
    `https://ig.gov-cloud.ai/tf-entity-ingestion/v1.0/schemas/${schemaId}/instances?upsert=true`,
    { method: "POST", headers: headers, body: JSON.stringify(data) }
  );
  if (!response.ok) {
    throw new Error(`API request failed with status ${response?.status}`);
  }
  const finalData = await response.json();
  return finalData.succeededCount;
}



app.get("/", async (req, res) => {
  try {
    res.send("service is running");
  } catch (error) {
    res.status(500).send(`Error occurred: ${error.message}`);
  }
});

app.get("/run", async (req, res) => {
  try {
    // Start the worker when the /run endpoint is hit
    const worker = new Worker("./webWorker.js");

    worker.on("message", async (data) => {
      try {
        let postResponse = await postDisastersData(
          "66bc46461b2d1c788b221897",
          data
        );
        console.log(`Posted ${postResponse} records`);
      } catch (error) {
        console.error(`Error posting data: ${error.message}`);
      }
    });

    worker.on("error", (error) => {
      console.error(`Worker error: ${error.message}`);
    });

    worker.on("exit", (code) => {
      if (code !== 0)
        console.error(`Worker stopped with exit code ${code}`);
    });

    res.send("Data processing started. Check logs for details.");
  } catch (error) {
    res.status(500).send(`Error occurred: ${error.message}`);
  }
});

app.listen(PORT, () => {
  console.log(`Server is running`);
});
