<!-- Project Architecture -->

BEYOND_ACADEMY/
├── config/
│   ├── __init__.py
│   ├── settings.py          # Environment configs (API keys, DB)
│   └── logger.py            # Logging setup
├── database/
│   ├── __init__.py
│   └── models.py            # SQLAlchemy models (Contacts, Accounts, InternRole)
├── zoho/
│   ├── __init__.py
│   ├── auth.py              # Access token lifecycle management
│   ├── api_client.py        # Reusable Zoho API client with pagination
│   └── extract.py           # Extractors for Contacts, Accounts, Roles
├── etl/
│   ├── __init__.py
│   └── pipeline.py          # Core ETL flow (extract-transform-load)
├── scripts/
│   └── run_etl.py           # Entrypoint to trigger the ETL pipeline
├── requirements.txt
├── README.md
└── .env
