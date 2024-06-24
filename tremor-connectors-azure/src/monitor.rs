// Copyright 2024, The Tremor Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(clippy::doc_markdown)]

//!
//! ## `azure_monitor_ingest_writer`
//!
//! The `azure_monitor_ingest_writer` makes it possible to write events to [Microsoft Azure Monitor Logs Ingestion API](https://learn.microsoft.com/en-us/azure/azure-monitor/logs/logs-ingestion-api-overview)
//! data collection rule [DCR](https://learn.microsoft.com/en-us/azure/azure-monitor/essentials/data-collection-rule-overview) endpoints.
//!
//! ### Configuration
//!
//! | option          | description                                                                                                           |
//! |-----------------|-----------------------------------------------------------------------------------------------------------------------|
//! | `auth`          | The client credentials authentication parameters required to authenticate with Azure                                  |
//! | `dce_base_url`  | The data collection endpoint base URL                                                                                 |
//! | `dcr`           | The immutable data collection rule id of the form `dcr-<uuid>` used as part of the ingest request                     |
//! | `stream`        | Refers to the stream name according to the data collection rule referenced                                            |
//! | `api_version`   | The API version this ingest request conforms to based on the data collection rule and monitor log table configuration |
//! | `concurrency`   | The number of concurrent requests to make to the Azure Monitor API. Default is 4                                      |
//! | `timeout`       | The timeout in milliseconds for the Azure Monitor API requests. Default is unset                                      |
//!
//!
//! ### Authentication
//!
//! | option          | description                                                                                                      |  
//! |-----------------|------------------------------------------------------------------------------------------------------------------|
//! | `client_id`     | The client ID of the Azure AD application used to authenticate with the Azure Monitor API                        |
//! | `client_secret` | The client secret of the Azure AD application used to authenticate with the Azure Monitor API                    |
//! | `tenant_id`     | The tenant ID of the Azure AD application used to authenticate with the Azure Monitor API                        |
//! | `scope`         | The scope of the Azure AD application used to authenticate with the Azure Monitor API                            |
//!
//! ### Example
//!
//! ```tremor
//!  define connector ingest from azure_monitor_ingest_writer
//!  with
//!    config = {
//!      "auth": {
//!        # We need this for client_credentials auth to get a bearer token
//!        "client_id": "abcd1234-abcd-4444-1234-abcd1234abcd",
//!        "client_secret": "BLOOQ~ZEEETZFN0RKWH1RL-CIRCLing-Splorq9",
//!        "tenant_id": "abcd1234-4567-4321-beef-f00df00dcafe",
//!      },
//!
//!      # We need this for the actual ingest api post request
//!      "dce_base_url": "https://my-configured-azure-data-collection-endpoint-name.northeurope-1.ingest.monitor.azure.com",
//!      "dcr": "dcr-f9f9c999edfdab77b444444444444549",
//!      "stream": "Custom-StreamName_CL",
//!      "api_version": "2023-01-01",
//!    }
//!  end;
//! ```
//!
//! #### Special considerations
//!
//! Note. Unlike the HTTP connectors, the Azure Monitor Ingestion connector does not use the HTTP authentication configuration
//! but instead uses a separate `auth` configuration object to provide the necessary credentials to authenticate with the
//! Azure Monitor API. Please raise an issue if you would like to see other Azure REST APIs supported.
//!
//! The data collection endpoint will always return a 204 No Content response, even if the request is malformed or the data is not accepted
//! by the data collection rule. The responsibility for ensuring the data is correctly formatted and accepted by the data collection rule
//! lies with the user. The connector will indicate a successful write if a 204 No Content response is received.
//!
//! We recommend checking the data collection endpoint metrics to ensure data is being received and checking the monitor logs
//! table to ensure the data is being correctly ingested and processed through the rule and into the target monitor log table.
//!
//! #### Azure
//!
//! To configure the Azure Monitor log collection API the following steps are required:
//! 1. [Create a Microsoft Entra application](https://learn.microsoft.com/en-en/azure/azure-monitor/logs/tutorial-logs-ingestion-api#create-azure-ad-application) to authenticate to the API.
//! 2. [Create a Data Collection Endpoint (DCE)](https://learn.microsoft.com/en-en/azure/azure-monitor/logs/tutorial-logs-ingestion-api#create-data-collection-endpoint) to receive data.
//! 3. [Create a custom table in a Log Analytics workspace](https://learn.microsoft.com/en-en/azure/azure-monitor/logs/tutorial-logs-ingestion-api#create-new-table-in-log-analytics-workspace) . This is the table you are sending data to.
//!   - It should also be possible to use a builtin table although an endopint and rule are still required for ingestion.
//! 4. [Create a data collection rule (DCR)](https://learn.microsoft.com/en-en/azure/azure-monitor/logs/tutorial-logs-ingestion-api#create-data-collection-rule) to route the data to the target table.
//! 5. [Grant the Microsoft Entra application access to the DCR.](https://learn.microsoft.com/en-en/azure/azure-monitor/logs/tutorial-logs-ingestion-api#assign-permissions-to-a-dcr)
//!
//! Curl, Postman, or other REST clients can be used to test the authentication and ingestion process
//! before configuring and testing the connector. An example using a curl compatible [Hurl](https://hurl.dev/) script is shown below:
//!
//! ````hurl
//! POST https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/token
//! Content-Type: application/x-www-form-urlencoded
//! ```
//! client_id=<client-id>&scope=https%3a%2f%2fmonitor.azure.com%2f%2f.default&client_secret=<client-secret>&grant_type=client_credentials
//! ```
//! HTTP 200
//! [Captures]
//! //! token: jsonpath "$['access_token']"
//!
//! POST https://<dce-name>.northeurope-1.ingest.monitor.azure.com/dataCollectionRules/<dcr-immutable-id>/streams/<dcr-stream-name>?api-version=2023-01-01
//! Content-Type: application/json
//! Authorization: Bearer {{token}}
//! ```json
//! [ { "TimeGenerated": "2024-06-05T16:55:44.6038417Z", "Kind": "manual", "Message": "Snot badger, hello form tremor", "TenantId": "<tenant-id>" } ]
//! ```
//! HTTP 204
//! ````

/// Microsoft Azure Monitor Ingestion API connectors
pub mod ingest;
