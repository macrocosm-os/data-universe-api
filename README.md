# S3 Storage for Data Universe with Folder-Based Access

This module provides S3 compatibility for the Data Universe subnet, allowing miners to upload data directly to S3 storage instead of (or alongside) HuggingFace. The implementation includes blockchain-based authentication and folder-based access for efficient uploads and downloads.

## Features

- **Folder-based access** using coldkey structure for miner isolation
- **Strong security** with policy-based restrictions and signature verification
- **Blockchain authentication** for miners and validators
- **Policy-based uploads** allowing efficient multi-file operations
- **Comprehensive validator access** to all miner data
- **Rate limiting** to prevent abuse
- **Compatible with HuggingFace** for smooth migration
- **FastAPI implementation** with automatic documentation
- **Docker support** for easy deployment

## Folder Structure

```
data/
├── x/  # X/Twitter data
│   ├── <miner_coldkey_1>/
│   │   ├── file1.parquet
│   │   ├── file2.parquet
│   │   └── ...
│   └── <miner_coldkey_2>/
│       ├── file1.parquet
│       └── ...
└── reddit/  # Reddit data
    ├── <miner_coldkey_1>/
    │   ├── file1.parquet
    │   └── ...
    └── <miner_coldkey_2>/
        └── ...
```

## Security Model

The security model ensures proper isolation between miners while giving validators complete access:

1. **Miner Isolation**: Each miner can only access their own folder (`data/<source>/<coldkey>/`)
2. **Policy Restrictions**: Upload policies restrict operations to specific folders
3. **Size Limits**: Files must be between 1KB and 5GB per upload
4. **Signature Verification**: All requests require valid blockchain signatures
5. **Time-Limited Access**: Access expires after 24 hours by default

## API Endpoints

### For Miners

- **`/get-folder-access`**: Get a policy for uploading multiple files to a folder
  - Creates a signed POST policy restricted to the miner's folder
  - Includes a listing URL for verifying uploads
  - Valid for 24 hours by default

### For Validators 

- **`/get-validator-access`**: Get comprehensive read access to all miner data
  - Provides organized URLs for global, source-level, and miner-specific access
  - Automatically discovers all miners in the system
  - Includes file listing and download capabilities

### Utility Endpoints

- **`/healthcheck`**: Check server status and configuration

## Setup Instructions

### Requirements

- Python 3.8+
- Required Python packages: `pip install -r requirements.txt`
- S3-compatible storage (AWS S3, MinIO, Cloudflare R2, etc.)
- AWS credentials with bucket access

### Configuration

Edit the `config.py` file to set your bucket and region:

```python
# S3 configuration
S3_BUCKET = 'your-bucket-name'  # Change to your bucket name
S3_REGION = 'us-east-1'         # Change to your region
SERVER_PORT = 8000              # Server port
```

### Running the Server

```bash
# Run the optimized FastAPI server
python -m uvicorn updated_server:app --host 0.0.0.0 --port 8000 --reload

# Or use Docker
docker build -t s3-auth-server .
docker run -p 8000:8000 s3-auth-server
```

## Testing

The repository includes several test utilities:

- **`cross_folder_test.py`**: Tests the security of folder isolation
- **`test_upload.py`**: Demonstrates miner upload workflows
- **`download_test.py`**: Tests validator download capabilities

Run tests with:

```bash
# Test folder security
python cross_folder_test.py

# Test upload capabilities
python test_upload.py

# Test validator capabilities
python download_test.py
```




## Migrating from HuggingFace

The recommended migration strategy:

1. **Dual Upload Phase**: Upload to both HuggingFace and S3
2. **Validator Update**: Update validators to check both systems
3. **S3-Only Phase**: Stop HuggingFace uploads once S3 is validated
4. **Historical Migration**: Move historical data as needed


## Security Best Practices

- Never expose AWS secret keys to clients
- Keep the authentication server behind a secure proxy
- Validate all inputs and enforce rate limits
- Monitor for unusual access patterns
- Regularly rotate AWS credentials
- Use the smallest set of S3 permissions needed

## Local Development & Testing

Boot up the test containers:
```
docker compose --env-file .test.env -f docker-compose.dev.yml up
```

Tear them down (optional remove volumes with -v)
```
docker compose -f docker-compose.dev.yml down -v
```

Run tests

```
pip install -r requirements-dev.txt
export PYTHONPATH=$(pwd)

pytest tests/ -s
```

For migrations, there's alembic.