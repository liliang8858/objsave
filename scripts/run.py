#!/usr/bin/env python3
"""
ObSave Service Runner
~~~~~~~~~~~~~~~~~~~~

This script starts the ObSave service with the specified configuration.
"""
import os
import sys
import argparse
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def parse_args():
    parser = argparse.ArgumentParser(description='Start the ObSave service')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=8000, help='Port to bind to')
    parser.add_argument('--reload', action='store_true', help='Enable auto-reload')
    parser.add_argument('--workers', type=int, default=1, help='Number of worker processes')
    return parser.parse_args()

def main():
    args = parse_args()
    
    # Import here to ensure project root is in Python path
    from obsave.api.app import create_app
    import uvicorn
    
    app = create_app()
    
    uvicorn.run(
        "obsave.api.app:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        workers=args.workers
    )

if __name__ == '__main__':
    main()
