import os
import subprocess
import tempfile
from typing import List, Dict, Any
import cv2
import numpy as np
import boto3
from botocore.exceptions import ClientError
from google.cloud import storage
import time
import logging

logger = logging.getLogger("ray.serve")

class VideoChunker:
    """Handles video chunking and frame extraction using FFmpeg and OpenCV."""
        
    def download_s3_video(self, s3_uri: str, output_dir: str = None) -> str:
        """
        Download a video from S3 and return the local file path.

        Args:
            s3_uri: S3 URI in format 's3://bucket/key' or just 'bucket/key'
            output_dir: Directory to save the video (defaults to temp directory)

        Returns:
            Local file path to the downloaded video
        """
        if output_dir is None:
            output_dir = tempfile.mkdtemp(prefix="s3_download_")

        # Parse S3 URI
        if s3_uri.startswith('s3://'):
            s3_uri = s3_uri[5:]  # Remove 's3://' prefix

        parts = s3_uri.split('/', 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid S3 URI: {s3_uri}. Expected format: 's3://bucket/key' or 'bucket/key'")

        bucket_name = parts[0]
        s3_key = parts[1]

        # Get filename from S3 key
        filename = os.path.basename(s3_key)
        local_path = os.path.join(output_dir, filename)

        # Download from S3
        s3_client = boto3.client('s3')

        try:
            print(f"Downloading from s3://{bucket_name}/{s3_key}...")
            s3_client.download_file(bucket_name, s3_key, local_path)
            print(f"Downloaded to: {local_path}")
            return local_path
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == '404' or error_code == 'NoSuchKey':
                raise RuntimeError(f"Video not found in S3: s3://{bucket_name}/{s3_key}")
            elif error_code == '403':
                raise RuntimeError(f"Access denied to S3 object: s3://{bucket_name}/{s3_key}")
            else:
                raise RuntimeError(f"Failed to download from S3: {e}")
        except Exception as e:
            raise RuntimeError(f"Failed to download from S3: {e}")

    def download_gcs_video(self, gcs_uri: str, output_dir: str = None) -> str:
        """
        Download a video from Google Cloud Storage and return the local file path.

        Args:
            gcs_uri: GCS URI in format 'gs://bucket/key' or just 'bucket/key'
            output_dir: Directory to save the video (defaults to temp directory)

        Returns:
            Local file path to the downloaded video
        """
        if output_dir is None:
            output_dir = tempfile.mkdtemp(prefix="gcs_download_")

        # Parse GCS URI
        if gcs_uri.startswith('gs://'):
            gcs_uri = gcs_uri[5:]  # Remove 'gs://' prefix

        parts = gcs_uri.split('/', 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid GCS URI: {gcs_uri}. Expected format: 'gs://bucket/key' or 'bucket/key'")

        bucket_name = parts[0]
        gcs_key = parts[1]

        # Get filename from GCS key
        filename = os.path.basename(gcs_key)
        local_path = os.path.join(output_dir, filename)

        try:
            print(f"Downloading from gs://{bucket_name}/{gcs_key}...")
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(gcs_key)

            blob.download_to_filename(local_path)
            print(f"Downloaded to: {local_path}")
            return local_path
        except Exception as e:
            if "404" in str(e) or "Not Found" in str(e):
                raise RuntimeError(f"Video not found in GCS: gs://{bucket_name}/{gcs_key}")
            elif "403" in str(e) or "Forbidden" in str(e):
                raise RuntimeError(f"Access denied to GCS object: gs://{bucket_name}/{gcs_key}")
            else:
                raise RuntimeError(f"Failed to download from GCS: {e}")

    def get_video_info(self, video_path: str) -> dict:
        """Get video metadata using FFprobe."""
        cmd = [
            'ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_format', '-show_streams',
            video_path
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            import json
            return json.loads(result.stdout)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to get video info: {e}")
    
    
    def chunk_video_to_memory(self, chunk_duration, video_input: str) -> List[Dict[str, Any]]:
        """
        Chunk a video to memory from cloud storage (S3 or GCS).

        Args:
            chunk_duration: Duration of each chunk in seconds
            video_input: Video ID (e.g., 'dQw4w9WgXcQ')
        """
        # Get cloud storage configuration from environment variables
        storage_path = os.environ.get('ANYSCALE_ARTIFACT_STORAGE')
        if not storage_path:
            raise ValueError("ANYSCALE_ARTIFACT_STORAGE environment variable not set")

        # Determine storage type and parse bucket/prefix
        is_gcs = storage_path.startswith('gs://')
        is_s3 = storage_path.startswith('s3://')

        if not (is_gcs or is_s3):
            raise ValueError("ANYSCALE_ARTIFACT_STORAGE must start with 'gs://' or 's3://'")

        # Remove the protocol prefix
        if is_gcs:
            clean_path = storage_path[5:]  # Remove 'gs://'
            protocol = 'gs'
        else:
            clean_path = storage_path[5:]  # Remove 's3://'
            protocol = 's3'

        parts = clean_path.split('/', 1)
        bucket_name = parts[0]
        prefix = parts[1] if len(parts) > 1 else ''

        # Construct URI from video ID
        if is_gcs:
            # For Google Cloud Storage, use the specific path structure
            # gs://ray-benchmark-data/serve_benchmark_videos/{video_id}.mp4
            key = f"serve_benchmark_videos/{video_input}.mp4"
            uri = f"gs://ray-benchmark-data/{key}"
        else:
            # For S3, use the original path structure
            key = f"{prefix}/videos/{video_input}.mp4"
            uri = f"{protocol}://{bucket_name}/{key}"

        # Download from the appropriate storage
        if is_gcs:
            video_path = self.download_gcs_video(uri)
        else:
            video_path = self.download_s3_video(uri)
        video_info = self.get_video_info(video_path)
        duration = float(video_info['format']['duration'])
        
        chunk_count = int(duration // chunk_duration) + 1
        chunk_data = []
        
        for i in range(chunk_count):
            start_time = i * chunk_duration
            
            # Create chunk in memory using FFmpeg
            cmd = [
                'ffmpeg', '-i', video_path,
                '-ss', str(start_time),
                '-t', str(chunk_duration),
                '-c', 'copy',  # Copy without re-encoding for speed
                '-avoid_negative_ts', 'make_zero',
                '-movflags', 'frag_keyframe+empty_moov',  # Enable streaming for MP4
                '-f', 'mp4',  # Output format
                'pipe:1'  # Output to stdout
            ]
            
            try:
                result = subprocess.run(cmd, capture_output=True, check=True)
                chunk_bytes = result.stdout
                
                if len(chunk_bytes) > 0:
                    chunk_data.append({
                        "chunk_id": i,
                        "chunk_bytes": chunk_bytes,
                        "start_time": start_time,
                        "duration": min(chunk_duration, duration - start_time),
                        "size_bytes": len(chunk_bytes)
                    })
                    
            except subprocess.CalledProcessError as e:
                print(f"Warning: Failed to create chunk {i}: {e}")
                continue
        
        # Clean up downloaded YouTube video if it was downloaded
        if video_path != video_input:
            try:
                os.remove(video_path)
                # Also remove the directory if it's empty
                video_dir = os.path.dirname(video_path)
                if os.path.exists(video_dir) and not os.listdir(video_dir):
                    os.rmdir(video_dir)
            except OSError:
                pass  # Ignore cleanup errors
        
        return chunk_data
    
    async def extract_frames_from_memory_chunk(self, chunk_id, chunk_data: Dict[str, Any], fps: float = 1.0) -> List[np.ndarray]:
        """
        Extract frames from an in-memory video chunk.
        
        Args:
            chunk_data: Dictionary containing chunk bytes and metadata
            fps: Frames per second to extract
        
        Returns:
            List of frames as numpy arrays
        """
        temp_file_path = None
        cap = None
        
        try:
            start = time.time()
            # Create a temporary file-like object from bytes
            chunk_bytes = chunk_data["chunk_bytes"]

            # Write to a temporary file that OpenCV can read
            with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as temp_file:
                temp_file.write(chunk_bytes)
                temp_file.flush()
                temp_file_path = temp_file.name
                
                # Use OpenCV to read the temporary file
                cap = cv2.VideoCapture(temp_file_path)
                frames = []
                
                if not cap.isOpened():
                    raise RuntimeError(f"Could not open video chunk in memory")
                
                frame_rate = cap.get(cv2.CAP_PROP_FPS)
                frame_interval = int(frame_rate / fps)
                frame_count = 0
                
                while True:
                    ret, frame = cap.read()
                    if not ret:
                        break
                    
                    if frame_count % frame_interval == 0:
                        frames.append(frame)
                    
                    frame_count += 1
                
                duration = time.time() - start
                logger.info(f"Decoded frames from memory chunk {chunk_id} took {duration:.3f} seconds")
                return frames
                
        except Exception as e:
            logger.error(f"Error extracting frames from memory chunk: {e}")
            return []
        finally:
            # Guaranteed cleanup - always executes
            if cap is not None:
                try:
                    cap.release()
                except Exception as e:
                    logger.warning(f"Error releasing video capture: {e}")
            
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.unlink(temp_file_path)
                    logger.debug(f"Cleaned up temporary file: {temp_file_path}")
                except OSError as e:
                    logger.warning(f"Error cleaning up temporary file {temp_file_path}: {e}")
                except Exception as e:
                    logger.warning(f"Unexpected error cleaning up temporary file {temp_file_path}: {e}")