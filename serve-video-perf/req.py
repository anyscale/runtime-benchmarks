import requests
import time
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor

# Simple function to process a video
def process_video(video_id, chunk_size=60, combined_inference=False):
    response = requests.get("http://localhost:8000/process-video",
                           params={"video_id": video_id, "chunk_size": chunk_size, "combined_inference": str(combined_inference).lower()})


    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None

# Async function to process a video
async def process_video_async(session, video_id, chunk_size=60, combined_inference=False):
    url = "http://localhost:8000/process-video"
    params = {"video_id": video_id, "chunk_size": chunk_size, "combined_inference": str(combined_inference).lower()}
    
    try:
        async with session.get(url, params=params) as response:
            if response.status == 200:
                return await response.json()
            else:
                text = await response.text()
                print(f"Error: {response.status} - {text}")
                return None
    except Exception as e:
        print(f"Request failed: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return None

def print_results(result, duration):
    """Print processing results in a consistent format."""
    print(f"Time taken: {duration:.3f} seconds")
    print(f"Chunks: {result.get('chunk_count', 0)}")
    print(f"Frames: {result.get('summary', {}).get('total_frames_processed', 0)}")

    # Show total data transferred
    total_size_mb = sum(chunk.get('size_mb', 0) for chunk in result.get('chunks', []))
    print(f"Total data transferred: {total_size_mb:.2f} MB")

    # Show top predictions
    for pred in result.get('summary', {}).get('top_predictions', [])[:5]:
        print(f"- {pred['prediction']['class']} ({pred['prediction']['confidence']:.3f})")
    print()

def process_and_print(video_id, description, chunk_size=60, combined_inference=False):
    """Process a video and print results."""
    print(f"\n{description}")
    print("=" * 50)
    start = time.time()
    result = process_video(video_id, chunk_size, combined_inference)

    if result:
        duration = time.time() - start
        print_results(result, duration)
    else:
        print("Failed to process video\n")

async def process_multiple_videos_parallel(video_requests, max_concurrent=4, combined_inference=False):
    """Process multiple videos in parallel with aiohttp."""
    print(f"\nProcessing {len(video_requests)} videos in parallel (max {max_concurrent} concurrent)")
    print("=" * 60)
    
    start_time = time.time()
    
    # Create semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def process_with_semaphore(session, video_id, description, chunk_size):
        async with semaphore:
            print(f"Starting: {description}")
            request_start = time.time()
            result = await process_video_async(session, video_id, chunk_size, combined_inference)
            request_duration = time.time() - request_start
            
            if result:
                print(f"Completed: {description} in {request_duration:.3f} seconds")
                return result, description, request_duration
            else:
                print(f"Failed: {description}")
                return None, description, request_duration
    
    # Create aiohttp session with longer timeouts and process all requests
    timeout = aiohttp.ClientTimeout(total=3600)  # 1 hour total timeout
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [
            process_with_semaphore(session, video_id, description, chunk_size)
            for video_id, description, chunk_size in video_requests
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
    
    total_duration = time.time() - start_time
    print(f"\nTotal time for all parallel requests: {total_duration:.3f} seconds")
    
    # Handle results and exceptions
    successful_results = []
    failed_count = 0
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"Task {i+1} raised exception: {type(result).__name__}: {result}")
            failed_count += 1
        elif isinstance(result, tuple) and result[0] is not None:
            successful_results.append(result)
        else:
            print(f"Task {i+1} returned None or invalid result")
            failed_count += 1
    
    # Print results for successful requests
    for result, description, duration in successful_results:
        print(f"\n{description} Results:")
        print("-" * 30)
        print_results(result, duration)
    
    # Calculate and print average latency
    if successful_results:
        durations = [duration for _, _, duration in successful_results]
        avg_latency = sum(durations) / len(durations)
        min_latency = min(durations)
        max_latency = max(durations)
        
        print(f"\n{'='*50}")
        print(f"PERFORMANCE SUMMARY")
        print(f"{'='*50}")
        print(f"Total requests: {len(video_requests)}")
        print(f"Successful requests: {len(successful_results)}")
        print(f"Failed requests: {failed_count}")
        print(f"Success rate: {len(successful_results)/len(video_requests)*100:.1f}%")
        print(f"Average latency: {avg_latency:.3f} seconds")
        print(f"Min latency: {min_latency:.3f} seconds")
        print(f"Max latency: {max_latency:.3f} seconds")
        print(f"Total processing time: {total_duration:.3f} seconds")
        print(f"{'='*50}")
    else:
        print(f"\n{'='*50}")
        print(f"PERFORMANCE SUMMARY")
        print(f"{'='*50}")
        print(f"Total requests: {len(video_requests)}")
        print(f"Successful requests: 0")
        print(f"Failed requests: {failed_count}")
        print(f"Success rate: 0.0%")
        print(f"Total processing time: {total_duration:.3f} seconds")
        print(f"{'='*50}")

# Example usage
if __name__ == "__main__":
    # Define video requests for parallel processing
    video_requests = [
        ("U9mJuUkhUzk", "Medium long video (1 hours) with 300s chunks - Request 1", 300),
        ("U9mJuUkhUzk", "Medium long video (1 hours) with 300s chunks - Request 2", 300),
        ("U9mJuUkhUzk", "Medium long video (1 hours) with 300s chunks - Request 3", 300),
        ("U9mJuUkhUzk", "Medium long video (1 hours) with 300s chunks - Request 4", 300),
        ("U9mJuUkhUzk", "Medium long video (1 hours) with 300s chunks - Request 5", 300),
        ("U9mJuUkhUzk", "Medium long video (1 hours) with 300s chunks - Request 6", 300),
        ("U9mJuUkhUzk", "Medium long video (1 hours) with 300s chunks - Request 7", 300),
        ("U9mJuUkhUzk", "Medium long video (1 hours) with 300s chunks - Request 8", 300),
        ("U9mJuUkhUzk", "Medium long video (1 hours) with 300s chunks - Request 9", 300),
        ("U9mJuUkhUzk", "Medium long video (1 hours) with 300s chunks - Request 10", 300),
        ("U9mJuUkhUzk", "Medium long video (1 hours) with 300s chunks - Request 11", 300),
        ("U9mJuUkhUzk", "Medium long video (1 hours) with 300s chunks - Request 12", 300),
    ]
    
    # Test both inference modes with a smaller set for comparison
    test_requests = video_requests[:12]  # Use first 3 requests for testing
    
    # Run parallel processing with chunk-by-chunk inference (default)
    print("=== CHUNK-BY-CHUNK INFERENCE ===")
    asyncio.run(process_multiple_videos_parallel(test_requests, max_concurrent=4, combined_inference=False))
    
    print("\n" + "="*80 + "\n")
    
    # Run parallel processing with combined inference
    print("=== COMBINED INFERENCE ===")
    asyncio.run(process_multiple_videos_parallel(test_requests, max_concurrent=4, combined_inference=True))