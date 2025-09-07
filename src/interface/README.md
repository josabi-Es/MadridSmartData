This project uses .env files to configure paths for your data. Follow these steps to set it up:

1. Copy the template to create your own .env file:

```
cp .env.template .env
```
2. Edit the .env file and set the paths to your processed data files. For example:

- Local files: Set the paths to the files you generated during preprocessing.

- HDFS files: Set the HDFS URI where you stored your processed data.

3. Place your processed files in the preprocessing folder or the appropriate path, and update .env with their location.

**Data Flow & Folder Structure**


                ┌────────────────────────────┐
                │       Processed Data       │
                │----------------------------│
                │ POLLUTION_PROCESSED_PATH   │
                │ TRAFFIC_PROCESSED_PATH     │
                │ DISTRITOS_FILTERED_PATH    │
                └────────────────────────────┘
                            │
                            ▼
               ┌────────────────────────────┐
               │          Interface         │
               │----------------------------│
               │           Gradio           │
               │ (User Interaction / GUI)   │
               └────────────────────────────┘
                            │
                            ▼
               ┌────────────────────────────┐
               │        Visualization       │
               │ (Plots, Maps, Time Series) │
               └────────────────────────────┘
