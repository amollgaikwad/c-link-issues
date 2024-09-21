import crypto from "crypto";
import { parentPort } from "worker_threads";

let urls = [
  {
    city: "Houston, TX",
    url: "https://seeclickfix.com/open311/v2/requests.json?lat=29.749907&long=-95.358421",
  },
  {
    city: "Detroit, MI",
    url: "https://seeclickfix.com/open311/v2/requests.json?lat=42.33143000&long=-83.04575000",
  },
  {
    city: "Miami, FL",
    url: "https://seeclickfix.com/open311/v2/requests.json?lat=25.77427000&long=-80.19366000",
  },
  {
    city: "Las Vegas, NV",
    url: "https://seeclickfix.com/open311/v2/requests.json?lat=36.188110&long=-115.176468",
  },
  {
    city: "Minneapolis, MN",
    url: "https://seeclickfix.com/open311/v2/requests.json?lat=44.97997&long=-93.26384",
  },
  {
    city: "Raleigh, NC",
    url: "https://seeclickfix.com/open311/v2/requests.json?lat=35.787743&long=-78.644257",
  },
  {
    city: "Salt Lake City, UT",
    url: "https://seeclickfix.com/open311/v2/requests.json?lat=40.758701&long=-111.876183",
  },
  {
    city: "Syracuse, NY",
    url: "https://seeclickfix.com/open311/v2/requests.json?lat=43.0481&long=-76.1474",
  },
  {
    city: "Birmingham, AL",
    url: "https://seeclickfix.com/open311/v2/requests.json?lat=33.543682&long=-86.779633",
  },
  {
    city: "Tucson, AZ",
    url: "https://seeclickfix.com/open311/v2/requests.json?lat=32.253460&long=-110.911789",
  },
];

function addKeysAndUsersToObjects(objectsArray, cityName) {
  const updatedArray = objectsArray.map((obj) => ({
    ...obj,
    city: cityName,
    issue_id: `${obj.service_request_id}${cityName}${crypto.randomUUID()}`,
    reported_from_clinks: false,
    created_at: new Date().toLocaleDateString("en-GB"),
  }));

  const finalArray = updatedArray.map((obj, index) => {
    if (index < 5) {
      return {
        ...obj,
        clinks_user_id: `${cityName}123456789`,
        reported_from_clinks: true,
        created_at: new Date().toLocaleDateString("en-GB"),
      };
    } else {
      return obj;
    }
  });

  const uniqueArray = finalArray.reduce((acc, current) => {
    const x = acc.find(
      (item) => item.service_request_id === current.service_request_id
    );
    if (!x) {
      return acc.concat([current]);
    } else {
      return acc;
    }
  }, []);

  return uniqueArray;
}

async function fetchDisastersData(url) {
  const response = await fetch(`${url}`);
  if (!response.ok) {
    throw new Error(`API request failed with status ${response?.status}`);
  }
  const data = await response.json();
  return data;
}

(async function main() {
    try {
      // Fetch data from all URLs concurrently
      const fetchPromises = urls.map(async (item) => {
        try {
          const response = await fetchDisastersData(item.url);
          const updatedObjectsArray = addKeysAndUsersToObjects(
            response,
            item.city
          );
          // Send the processed data back to the parent thread
          parentPort.postMessage(updatedObjectsArray);
        } catch (error) {
          // Send error message to the parent thread
          parentPort.postMessage({ city: item.city, error: error.message });
        }
      });
  
      // Wait for all promises to complete
      await Promise.all(fetchPromises);
    } catch (error) {
      // General catch block for unforeseen errors
      parentPort.postMessage({ error: error.message });
    }
  })();
