"""
dbt integration assets for municipality analytics pipeline.

Handles the execution of dbt transformations as part of the Dagster pipeline.
Uses a file-based communication system with the dbt container to execute
all models in the proper sequence: staging → intermediate → marts.
"""

import os
import time
from dagster import asset, Output, AssetExecutionContext


# dbt project configuration
DBT_PROJECT_PATH = "/opt/dagster/dbt"


@asset(deps=["load_demography_to_postgres", "validate_codes_data"])
def municipality_dbt_models(context: AssetExecutionContext) -> Output[dict]:
    """
    Execute all dbt models in the municipality analytics project.
    
    This asset triggers the dbt transformation layer after all raw data
    has been successfully loaded and validated. It uses a file-based
    communication system with the dbt container to ensure reliable execution.
    
    Execution sequence:
    1. Creates a trigger file to signal the dbt container
    2. Waits for the dbt container to process the request
    3. Monitors for completion via result file
    4. Returns execution results and status
    
    The dbt container runs the following transformations:
    - Staging models: Clean atomic data from raw sources
    - Intermediate models: Business logic and data joining
    - Marts models: Wide, denormalized tables for analytics
    
    Dependencies:
        - load_demography_to_postgres: Raw demographic data must be loaded
        - validate_codes_data: Geographic reference data must be validated
    
    Returns:
        Output containing dbt execution results and status
    """
    context.log.info("Triggering dbt build process via file system communication")
    
    try:
        # Define file paths for container communication
        trigger_file = f"{DBT_PROJECT_PATH}/run_dbt_build.trigger"
        result_file = f"{DBT_PROJECT_PATH}/dbt_build_result.txt"
        
        # Remove any existing result file to avoid stale results
        if os.path.exists(result_file):
            os.remove(result_file)
            context.log.info("Removed existing result file")
        
        # Create trigger file with metadata
        with open(trigger_file, 'w') as f:
            f.write(f"dbt_build_requested_at_{int(time.time())}\n")
            f.write("requested_by=dagster_pipeline\n")
            f.write("command=dbt_build\n")
            f.write("dependencies_completed=load_demography_to_postgres,validate_codes_data\n")
        
        context.log.info(f"Created trigger file: {trigger_file}")
        
        # Wait for the dbt container to process the request
        max_wait_time = 300  # 5 minutes timeout
        check_interval = 10  # Check every 10 seconds
        waited_time = 0
        
        context.log.info(f"Waiting for dbt container to process build request (max {max_wait_time}s)")
        
        while waited_time < max_wait_time:
            if os.path.exists(result_file):
                context.log.info("dbt build result file found")
                break
            
            time.sleep(check_interval)
            waited_time += check_interval
            
            if waited_time % 30 == 0:  # Log progress every 30 seconds
                context.log.info(f"Still waiting for dbt build completion... ({waited_time}s elapsed)")
        
        # Check if we timed out
        if not os.path.exists(result_file):
            context.log.error(f"dbt build timed out after {max_wait_time} seconds")
            return Output(
                {
                    "status": "timeout",
                    "message": f"dbt build timed out after {max_wait_time} seconds",
                    "waited_time_seconds": waited_time
                },
                metadata={
                    "execution_method": "file_trigger",
                    "status": "timeout",
                    "timeout_seconds": max_wait_time
                }
            )
        
        # Read the result file
        with open(result_file, 'r') as f:
            result_content = f.read()
        
        context.log.info("dbt build completed")
        context.log.info(f"dbt execution result preview: {result_content[:500]}...")
        
        # Determine if the build was successful based on result content
        build_successful = "SUCCESS:" in result_content
        
        # Clean up temporary files
        try:
            if os.path.exists(trigger_file):
                os.remove(trigger_file)
            if os.path.exists(result_file):
                os.remove(result_file)
            context.log.info("Cleaned up communication files")
        except Exception as cleanup_error:
            context.log.warning(f"Failed to clean up files: {cleanup_error}")
        
        # Parse result content for additional metadata
        metadata = _parse_dbt_result(result_content)
        
        return Output(
            {
                "status": "success" if build_successful else "failed",
                "result": result_content,
                "execution_method": "file_trigger",
                "build_successful": build_successful,
                "waited_time_seconds": waited_time
            },
            metadata={
                "execution_method": "file_trigger",
                "wait_time_seconds": waited_time,
                "build_successful": build_successful,
                **metadata
            }
        )
        
    except Exception as e:
        context.log.error(f"Error triggering dbt build: {e}")
        import traceback
        context.log.error(f"Traceback: {traceback.format_exc()}")
        
        return Output(
            {
                "status": "error",
                "error": str(e),
                "execution_method": "file_trigger"
            },
            metadata={
                "execution_method": "file_trigger",
                "error": str(e),
                "build_successful": False
            }
        )


def _parse_dbt_result(result_content: str) -> dict:
    """
    Parse dbt result content to extract useful metadata.
    
    Args:
        result_content: Raw output from dbt build command
        
    Returns:
        Dictionary containing parsed metadata
    """
    metadata = {}
    
    try:
        lines = result_content.split('\n')
        
        # Look for summary line (e.g., "Done. PASS=27 WARN=0 ERROR=2 SKIP=0 TOTAL=29")
        for line in lines:
            if 'Done.' in line and 'PASS=' in line:
                # Extract test/model counts
                parts = line.split()
                for part in parts:
                    if '=' in part:
                        key, value = part.split('=')
                        try:
                            metadata[f"dbt_{key.lower()}"] = int(value)
                        except ValueError:
                            pass
                break
        
        # Check for specific error indicators
        if 'FAILED:' in result_content:
            metadata['has_failures'] = True
        if 'ERROR:' in result_content:
            metadata['has_errors'] = True
        if 'WARNING:' in result_content:
            metadata['has_warnings'] = True
            
        # Count models mentioned
        model_count = result_content.count(' model ')
        if model_count > 0:
            metadata['models_mentioned'] = model_count
            
    except Exception as e:
        metadata['parse_error'] = str(e)
    
    return metadata