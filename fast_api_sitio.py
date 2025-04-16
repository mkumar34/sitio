from fastapi import FastAPI, HTTPException, Request, Depends, status
from fastapi.security import APIKeyHeader
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import logging
import subprocess
import json
import os
import ipaddress
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
import uvicorn

# Initialize FastAPI
app = FastAPI(title="Script Runner API", version="1.0")

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Define the scripts directory
SCRIPTS_DIR = "C:\\Users\\DAK_Kumar\\Documents\\Fast_api"

# Define allowed IP addresses/ranges
ALLOWED_IPS = [
    "10.100.0.0/16",  # Covers all Databricks subnets
    "10.100.0.0/24",  # Public subnet for Dev
    "10.100.1.0/24",  # Private subnet for Dev
    "10.100.3.0/24"   # Private subnet for Prod
]

# Rate limiting configuration
RATE_LIMIT_TOKENS = 10  # Maximum number of tokens per client
RATE_LIMIT_REFILL_RATE = 5  # Tokens refilled per second
RATE_LIMIT_STORAGE: Dict[str, Tuple[float, int]] = {}  # IP -> (last_update_time, tokens)
rate_limit_lock = threading.Lock()

# API Key configuration
API_KEY_NAME = "X-API-Key"
API_KEY_HEADER = APIKeyHeader(name=API_KEY_NAME, auto_error=True)
# Define your approved API keys
API_KEYS = {
    "2y3rqUJ2-Z4rbhcu1r433OWlSF31QlcmLCC3heQsK2Q" 
}

# API key dependency
async def get_api_key(api_key: str = Depends(API_KEY_HEADER)):
    if api_key not in API_KEYS:
        logger.warning(f"Invalid API key used: {api_key[:5]}...")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key"
        )
    return api_key

# Rate limiting middleware
@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    client_ip = request.client.host
    endpoint = request.url.path
    
    # Different limits for different endpoints
    if endpoint == "/run-script":
        tokens_max = RATE_LIMIT_TOKENS
        refill_rate = RATE_LIMIT_REFILL_RATE
    else:
        tokens_max = 50  # Higher for non-resource-intensive endpoints
        refill_rate = 10
    
    with rate_limit_lock:
        current_time = time.time()
        
        # Initialize if client is new
        if client_ip not in RATE_LIMIT_STORAGE:
            RATE_LIMIT_STORAGE[client_ip] = (current_time, tokens_max)
            logger.info(f"New client {client_ip} initialized with {tokens_max} tokens")
        
        # Get last update time and tokens
        last_update, tokens = RATE_LIMIT_STORAGE[client_ip]
        
        # Calculate token refill based on time passed
        time_passed = current_time - last_update
        token_refill = time_passed * refill_rate
        
        # Update tokens (up to max limit)
        new_tokens = min(tokens_max, tokens + token_refill)
        
        # If no tokens left, return 429 Too Many Requests
        if new_tokens < 1:
            wait_time = (1 - new_tokens) / refill_rate
            RATE_LIMIT_STORAGE[client_ip] = (current_time, new_tokens)
            logger.warning(f"Rate limit exceeded for {client_ip}. Retry after {wait_time:.1f} seconds")
            return JSONResponse(
                status_code=429,
                content={
                    "detail": f"Rate limit exceeded. Try again in {wait_time:.1f} seconds.",
                    "retry_after": int(wait_time) + 1
                },
                headers={"Retry-After": str(int(wait_time) + 1)}
            )
        
        # Consume a token and update storage
        RATE_LIMIT_STORAGE[client_ip] = (current_time, new_tokens - 1)
        logger.info(f"Client {client_ip} has {new_tokens - 1:.1f} tokens remaining")
    
    # Continue with the request
    response = await call_next(request)
    return response

# IP whitelist middleware
@app.middleware("http")
async def ip_whitelist_middleware(request: Request, call_next):
    client_ip = request.client.host
    logger.info(f"Request from IP: {client_ip}")
    
    # Check if IP is allowed
    ip_allowed = False
    
    for allowed_ip in ALLOWED_IPS:
        if "/" in allowed_ip:  # It's a CIDR notation
            try:
                network = ipaddress.ip_network(allowed_ip)
                client_ip_obj = ipaddress.ip_address(client_ip)
                if client_ip_obj in network:
                    ip_allowed = True
                    logger.info(f"IP {client_ip} is in allowed network {allowed_ip}")
                    break
            except Exception as e:
                logger.error(f"Error checking network range {allowed_ip}: {e}")
        elif client_ip == allowed_ip:  # Direct IP match
            ip_allowed = True
            logger.info(f"IP {client_ip} directly matches allowed IP {allowed_ip}")
            break
    
    if not ip_allowed:
        logger.warning(f"Blocked request from unauthorized IP: {client_ip}")
        return JSONResponse(
            status_code=403, 
            content={"detail": "Access denied: Your IP is not whitelisted"}
        )
    
    # If IP is allowed, proceed with the request
    response = await call_next(request)
    return response

# Request logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    client_ip = request.client.host
    method = request.method
    path = request.url.path
    
    forwarded_for = request.headers.get("X-Forwarded-For")
    user_agent = request.headers.get("User-Agent")
    
    logger.info(f"Request: {method} {path} - IP: {client_ip}, X-Forwarded-For: {forwarded_for}, User-Agent: {user_agent}")
    
    response = await call_next(request)
    
    process_time = time.time() - start_time
    logger.info(f"Response: {method} {path} - Status: {response.status_code}, Time: {process_time:.3f}s")
    
    return response

# Define model for script request with parameters
class ScriptRequest(BaseModel):
    script_name: str
    parameters: Optional[Dict[str, Any]] = None

# Health check endpoint
@app.get("/health")
def health_check(api_key: str = Depends(get_api_key)):
    logger.info(f"Health check called with valid API key")
    return {"status": "ok", "authenticated": True}

# Root endpoint
@app.get("/")
def root(api_key: str = Depends(get_api_key)):
    logger.info(f"Root endpoint called with valid API key")
    return {"message": "Welcome to Script Runner API!"}

# Endpoint to run a Python script with parameters
@app.post("/run-script")
def run_script(request: ScriptRequest, api_key: str = Depends(get_api_key)):
    script_name = request.script_name
    script_path = os.path.join(SCRIPTS_DIR, script_name)
    parameters = request.parameters or {}
    
    logger.info(f"Running script: {script_path} with parameters: {parameters}")
    
    # Security check: Prevent path traversal
    if ".." in script_name or script_name.startswith("/") or script_name.startswith("\\"):
        logger.error(f"Path traversal attempt: {script_name}")
        raise HTTPException(status_code=400, detail="Invalid script name")
    
    # Check if script exists
    if not os.path.exists(script_path):
        logger.error(f"Script not found: {script_path}")
        raise HTTPException(status_code=404, detail=f"Script not found: {script_name}")
    
    try:
        # Prepare command with parameters
        command = ['python', script_path]
        
        # Add parameters as command line arguments with sanitization
        for key, value in parameters.items():
            if value is not None:  # Only add if value is not None
                # Sanitize value to prevent command injection
                safe_value = str(value).replace(";", "").replace("|", "").replace("&", "")
                command.append(f'--{key}={safe_value}')
        
        logger.info(f"Executing command: {' '.join(command)}")
        
        # Run the script and capture output
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=300  # 5-minute timeout
        )
        
        # Log the results
        logger.info(f"Script execution completed with return code: {result.returncode}")
        if result.stdout:
            logger.info(f"Script output preview: {result.stdout[:200]}...")
        if result.stderr:
            logger.error(f"Script errors: {result.stderr}")
        
        # Try to parse output as JSON
        output_data = None
        try:
            if result.stdout:
                output_data = json.loads(result.stdout)
        except json.JSONDecodeError:
            logger.warning(f"Could not parse output as JSON: {result.stdout[:200]}...")
            output_data = None
        
        # Prepare response
        response = {
            "script_name": script_name,
            "parameters": parameters,
            "return_code": result.returncode,
            "success": result.returncode == 0,
            "output": output_data if output_data else result.stdout,
            "error": result.stderr if result.stderr else None
        }
        
        return response
        
    except subprocess.TimeoutExpired:
        logger.error(f"Script execution timed out: {script_name}")
        raise HTTPException(status_code=408, detail="Script execution timed out")
    except Exception as e:
        logger.error(f"Error executing script: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint to list available scripts
@app.get("/list-scripts")
def list_scripts(api_key: str = Depends(get_api_key)):
    logger.info(f"Listing scripts with valid API key")
    try:
        scripts = []
        for file in os.listdir(SCRIPTS_DIR):
            if file.endswith(".py"):
                script_path = os.path.join(SCRIPTS_DIR, file)
                script_info = {
                    "name": file,
                    "path": script_path,
                    "size": os.path.getsize(script_path)
                }
                
                # Try to extract parameters by running with --help
                try:
                    help_result = subprocess.run(
                        ['python', script_path, '--help'],
                        capture_output=True,
                        text=True,
                        timeout=5  # Timeout after 5 seconds
                    )
                    if help_result.stdout:
                        script_info["help_text"] = help_result.stdout
                except Exception as e:
                    logger.warning(f"Could not get help text for {file}: {e}")
                
                scripts.append(script_info)
                
        return {"scripts": scripts}
    except Exception as e:
        logger.error(f"Error listing scripts: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Get script metadata (available parameters)
@app.get("/script-info/{script_name}")
def get_script_info(script_name: str, api_key: str = Depends(get_api_key)):
    logger.info(f"Getting info for script: {script_name}")
    
    # Security check: Prevent path traversal
    if ".." in script_name or script_name.startswith("/") or script_name.startswith("\\"):
        logger.error(f"Path traversal attempt: {script_name}")
        raise HTTPException(status_code=400, detail="Invalid script name")
        
    script_path = os.path.join(SCRIPTS_DIR, script_name)
    
    # Check if script exists
    if not os.path.exists(script_path):
        logger.error(f"Script not found: {script_path}")
        raise HTTPException(status_code=404, detail=f"Script not found: {script_name}")
    
    try:
        # Run script with --help flag to get parameter info
        result = subprocess.run(
            ['python', script_path, '--help'],
            capture_output=True,
            text=True,
            timeout=5  # Timeout after 5 seconds
        )
        
        help_text = result.stdout
        
        # Basic metadata
        metadata = {
            "name": script_name,
            "path": script_path,
            "size": os.path.getsize(script_path),
            "help_text": help_text,
            "modified_time": os.path.getmtime(script_path)
        }
        
        return metadata
    except Exception as e:
        logger.error(f"Error getting script info: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Run the application
if __name__ == "__main__":
    host = "10.223.106.19"
    port = 8000
    
    logger.info(f"Starting FastAPI on {host}:{port}")
    logger.info(f"IP Whitelisting enabled for: {', '.join(ALLOWED_IPS)}")
    logger.info(f"Rate limiting: {RATE_LIMIT_TOKENS} tokens per client, refill at {RATE_LIMIT_REFILL_RATE} per second")
    logger.info(f"API Key authentication enabled")
    
    uvicorn.run(app, host=host, port=port)