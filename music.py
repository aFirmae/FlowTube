import os
import uuid
import json
import time
import asyncio
import zipfile
import shutil
from contextlib import asynccontextmanager
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import yt_dlp

# Defensive configuration: Ensure /opt/homebrew/bin is in PATH (common for macOS ffmpeg installation)
os.environ["PATH"] = os.environ.get("PATH", "") + ":/opt/homebrew/bin:/usr/local/bin"

# Global configurations
DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", "downloads")

# Global dictionary to track active downloads
# Schema: {
#   task_id: {
#     "status": "pending" | "downloading" | "postprocessing" | "zipping" | "completed" | "failed",
#     "queue": asyncio.Queue(),
#     "final_file_path": str,
#     "display_filename": str,
#     "error": str,
#     "created_at": float
#   }
# }
tasks: Dict[str, Dict[str, Any]] = {}

async def periodic_cleanup():
    """Periodically cleans up downloaded files older than 30 minutes and tasks older than 1 hour."""
    while True:
        await asyncio.sleep(300)  # run every 5 minutes
        now = time.time()
        
        # 1. Clean up the downloads folder
        if os.path.exists(DOWNLOAD_DIR):
            for item in os.listdir(DOWNLOAD_DIR):
                item_path = os.path.join(DOWNLOAD_DIR, item)
                try:
                    mtime = os.path.getmtime(item_path)
                    # Delete files or folders older than 30 minutes (1800 seconds)
                    if now - mtime > 1800:
                        if os.path.isdir(item_path):
                            shutil.rmtree(item_path)
                        else:
                            os.remove(item_path)
                        print(f"Periodic cleanup: removed {item_path}")
                except Exception as e:
                    print(f"Periodic cleanup error on file {item_path}: {e}")
                    
        # 2. Clean up task states from memory older than 1 hour (3600 seconds)
        to_delete = []
        for tid, tinfo in tasks.items():
            if now - tinfo.get("created_at", now) > 3600:
                to_delete.append(tid)
        for tid in to_delete:
            tasks.pop(tid, None)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ensure downloads directory exists
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    # Start periodic cleanup task
    cleanup_task = asyncio.create_task(periodic_cleanup())
    yield
    # Cancel cleanup task on shutdown
    cleanup_task.cancel()

app = FastAPI(
    title="FlowTube",
    description="Download YouTube videos and playlists as MP3/MP4",
    lifespan=lifespan
)

# Enable CORS for local development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class AnalyzeRequest(BaseModel):
    url: str

class DownloadRequest(BaseModel):
    url: str
    video_ids: Optional[List[str]] = None
    video_titles: Optional[List[str]] = None
    format: str  # "mp3" or "mp4"

class AbortRequest(BaseModel):
    index: Optional[int] = None
    keep_files: Optional[bool] = False

class DownloadAborted(Exception): 
    pass

@app.post("/api/abort/{task_id}")
async def abort_download(task_id: str, payload: AbortRequest):
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="Task not found")
        
    task_info = tasks[task_id]
    
    if payload.index is not None:
        if "aborted_items" not in task_info:
            task_info["aborted_items"] = set()
        task_info["aborted_items"].add(payload.index)
        return {"detail": f"Aborted item at index {payload.index}"}
    else:
        task_info["aborted_all"] = True
        task_info["keep_files"] = payload.keep_files
        return {"detail": "Aborted entire task"}

async def get_info(url: str) -> dict:
    """Uses yt-dlp to extract flat metadata of a video or playlist without downloading."""
    def _extract():
        ydl_opts = {
            'extract_flat': True,
            'skip_download': True,
            'extractor_args': {'youtube': {'client': ['ios']}},
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            return ydl.extract_info(url, download=False)
            
    return await asyncio.to_thread(_extract)

def download_blocking(task_id: str, urls: List[str], format_choice: str, loop: asyncio.AbstractEventLoop, queue: asyncio.Queue, titles: Optional[List[str]] = None):
    """Blocking function to download videos. Executed inside a thread pool via asyncio.to_thread."""
    temp_dir = os.path.join(DOWNLOAD_DIR, task_id)
    os.makedirs(temp_dir, exist_ok=True)
    
    total = len(urls)
    failed_items = []
    
    def send_msg(status: str, **kwargs):
        msg = {
            "status": status,
            "task_id": task_id,
            "total_items": total,
            **kwargs
        }
        # Safely schedule the queue insertion in the main event loop from this worker thread
        loop.call_soon_threadsafe(queue.put_nowait, msg)
    
    # Factory function to properly capture title and index in closure
    def make_progress_hook(vid_title, vid_index):
        def progress_hook(d):
            task_info = tasks.get(task_id, {})
            if task_info.get("aborted_all", False) or vid_index in task_info.get("aborted_items", set()):
                raise DownloadAborted("ABORTED_BY_USER")

            if d['status'] == 'downloading':
                dl_bytes = d.get('downloaded_bytes', 0)
                tot_bytes = d.get('total_bytes') or d.get('total_bytes_estimate') or 0
                pct = (dl_bytes / tot_bytes * 100) if tot_bytes > 0 else 0
                speed = d.get('speed')
                speed_str = f"{speed / 1024 / 1024:.2f} MB/s" if speed else ""
                eta = d.get('eta')
                eta_str = f"{eta}s" if eta else ""
                
                send_msg(
                    "progress", 
                    title=vid_title, 
                    index=vid_index, 
                    percent=round(pct, 1), 
                    speed=speed_str, 
                    eta=eta_str
                )
            elif d['status'] == 'finished':
                send_msg("postprocessing", title=vid_title, index=vid_index)
        return progress_hook
        
    for idx, url in enumerate(urls):
        current_index = idx + 1
        
        task_info = tasks.get(task_id, {})
        if task_info.get("aborted_all", False):
            break
            
        if current_index in task_info.get("aborted_items", set()):
            send_msg("item_aborted", title=f"Video {current_index}", index=current_index)
            continue
        
        title = titles[idx] if titles and idx < len(titles) else None
        
        if not title:
            # 1. Fetch the title of the video first (fast step)
            title_opts = {
                'skip_download': True,
                'extractor_args': {'youtube': {'client': ['ios']}},
            }
            try:
                with yt_dlp.YoutubeDL(title_opts) as ydl:
                    info = ydl.extract_info(url, download=False)
                    title = info.get('title', f"Video {current_index}")
            except Exception:
                title = f"Video {current_index}"
            
        send_msg("starting_item", title=title, index=current_index)
        
        # 3. Setup yt_dlp for downloading this item
        ydl_opts = {
            'outtmpl': os.path.join(temp_dir, '%(title)s.%(ext)s'),
            'progress_hooks': [make_progress_hook(title, current_index)],
            'sleep_interval': 2,
            'max_sleep_interval': 5,
            'extractor_args': {'youtube': {'client': ['ios']}},
        }
        
        if format_choice == 'mp3':
            ydl_opts.update({
                'format': 'bestaudio/best',
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': '192',
                }],
            })
        else:
            # Download best mp4 available or merge best video + best audio
            ydl_opts.update({
                'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
                'merge_output_format': 'mp4',
            })
            
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])
            # Notify frontend that this specific video completed successfully
            send_msg("item_complete", title=title, index=current_index)
        except Exception as e:
            if "ABORTED_BY_USER" in str(e):
                send_msg("item_aborted", title=title, index=current_index)
                if tasks.get(task_id, {}).get("aborted_all", False):
                    break
                else:
                    continue

            error_msg = f"Failed to download '{title}': {str(e)}"
            print(error_msg)
            failed_items.append({"title": title, "index": current_index, "error": error_msg})
            send_msg("item_failed", title=title, index=current_index, error=error_msg)
            # Continue with remaining videos instead of aborting the entire batch
            continue
            
    # All download attempts finished or broke. Gather successfully downloaded files.
    task_info = tasks.get(task_id, {})
    aborted_all = task_info.get("aborted_all", False)
    keep_files = task_info.get("keep_files", False)

    if aborted_all and not keep_files:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        send_msg("aborted", error="Download aborted by user.")
        if task_id in tasks:
            tasks[task_id]["status"] = "aborted"
            tasks[task_id]["error"] = "Download aborted by user."
        return

    files = []
    if os.path.exists(temp_dir):
        for root, dirs, filenames in os.walk(temp_dir):
            for filename in filenames:
                if not filename.startswith('.'):
                    files.append(os.path.join(root, filename))
                    
    if not files:
        if aborted_all:
            error_msg = "Download aborted before any files completed."
            status_event = "aborted"
        else:
            error_msg = "No files were downloaded successfully."
            if failed_items:
                error_msg = f"All {len(failed_items)} video(s) failed to download."
            status_event = "failed"
            
        send_msg(status_event, error=error_msg)
        if task_id in tasks:
            tasks[task_id]["status"] = status_event
            tasks[task_id]["error"] = error_msg
        return
        
    # Process output files
    if len(files) > 1:
        send_msg("zipping")
        zip_path = os.path.join(DOWNLOAD_DIR, f"{task_id}.zip")
        try:
            with zipfile.ZipFile(zip_path, 'w') as zipf:
                for file_path in files:
                    zipf.write(file_path, arcname=os.path.basename(file_path))
            
            # Clean up temp_dir immediately after zipping
            shutil.rmtree(temp_dir)
            
            if task_id in tasks:
                tasks[task_id]["status"] = "completed"
                tasks[task_id]["final_file_path"] = zip_path
                tasks[task_id]["display_filename"] = "downloads.zip"
            send_msg("completed", download_url=f"/api/download/file/{task_id}")
        except Exception as e:
            error_msg = f"Failed to package files into ZIP: {str(e)}"
            send_msg("failed", error=error_msg)
            if task_id in tasks:
                tasks[task_id]["status"] = "failed"
                tasks[task_id]["error"] = error_msg
            return
    else:
        # Exactly one file downloaded
        single_file = files[0]
        ext = os.path.splitext(single_file)[1]
        dest_path = os.path.join(DOWNLOAD_DIR, f"{task_id}{ext}")
        
        try:
            shutil.move(single_file, dest_path)
            shutil.rmtree(temp_dir)
            
            if task_id in tasks:
                tasks[task_id]["status"] = "completed"
                tasks[task_id]["final_file_path"] = dest_path
                tasks[task_id]["display_filename"] = os.path.basename(single_file)
            send_msg("completed", download_url=f"/api/download/file/{task_id}")
        except Exception as e:
            error_msg = f"Failed to relocate output file: {str(e)}"
            send_msg("failed", error=error_msg)
            if task_id in tasks:
                tasks[task_id]["status"] = "failed"
                tasks[task_id]["error"] = error_msg
            return

async def run_download_task(task_id: str, urls: List[str], format_choice: str, titles: Optional[List[str]] = None):
    """Runs the blocking download process in a background thread."""
    loop = asyncio.get_running_loop()
    queue = tasks[task_id]["queue"]
    
    try:
        await asyncio.to_thread(download_blocking, task_id, urls, format_choice, loop, queue, titles)
    except Exception as e:
        print(f"Task {task_id} failed: {e}")

@app.post("/api/analyze")
async def analyze_url_endpoint(request: AnalyzeRequest):
    url = request.url.strip()
    if not url:
        raise HTTPException(status_code=400, detail="URL cannot be empty")
        
    try:
        info = await get_info(url)
        if not info:
            raise HTTPException(status_code=400, detail="Failed to fetch video/playlist information.")
            
        extractor = info.get('extractor_key') or info.get('extractor') or "YouTube"
        if isinstance(extractor, str):
            if 'youtube' in extractor.lower():
                extractor = 'YouTube'
            elif 'soundcloud' in extractor.lower():
                extractor = 'SoundCloud'
            else:
                extractor = extractor.title()
            
        is_playlist = info.get('_type') == 'playlist'
        
        if is_playlist:
            raw_entries = info.get('entries', [])
            videos = []
            for entry in raw_entries:
                if not entry:
                    continue
                vid_id = entry.get('id')
                if not vid_id:
                    continue
                
                # Deduce thumbnail URL
                thumb_url = f"https://i.ytimg.com/vi/{vid_id}/mqdefault.jpg"
                if entry.get('thumbnails'):
                    thumb_url = entry['thumbnails'][0].get('url', thumb_url)
                
                title = entry.get('title') or ""
                uploader = entry.get('uploader') or entry.get('channel') or "Unknown Channel"
                
                # Determine if the video is private, deleted, or unavailable
                is_available = True
                if (not title or 
                    "[private video]" in title.lower() or 
                    "[deleted video]" in title.lower() or 
                    "[unavailable video]" in title.lower() or
                    title.lower() == "private video" or
                    title.lower() == "deleted video" or
                    uploader == "Unknown Channel"):
                    is_available = False
                    
                videos.append({
                    "id": vid_id,
                    "title": title or "Unavailable Video",
                    "thumbnail": thumb_url,
                    "duration": entry.get('duration'),
                    "uploader": uploader,
                    "url": f"https://www.youtube.com/watch?v={vid_id}",
                    "is_available": is_available
                })
                
            playlist_thumb = ""
            if videos:
                playlist_thumb = videos[0]["thumbnail"]
                
            return {
                "success": True,
                "type": "playlist",
                "id": info.get('id'),
                "title": info.get('title') or "Untitled Playlist",
                "thumbnail": playlist_thumb,
                "videos_count": len(videos),
                "videos": videos,
                "url": url,
                "extractor": extractor,
                "uploader": info.get('uploader') or info.get('channel') or "Unknown Channel"
            }
        else:
            # Single video
            vid_id = info.get('id')
            if not vid_id:
                raise HTTPException(status_code=400, detail="Could not extract video ID.")
                
            thumb_url = f"https://i.ytimg.com/vi/{vid_id}/maxresdefault.jpg"
            if info.get('thumbnails'):
                # Grab the largest available thumbnail
                thumb_url = info['thumbnails'][-1].get('url', thumb_url)
                
            return {
                "success": True,
                "type": "video",
                "id": vid_id,
                "title": info.get('title') or "Untitled Video",
                "thumbnail": thumb_url,
                "duration": info.get('duration'),
                "uploader": info.get('uploader') or info.get('channel') or "Unknown Channel",
                "url": url,
                "extractor": extractor
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/download")
async def start_download(request: DownloadRequest, background_tasks: BackgroundTasks):
    url = request.url
    
    urls_to_download = []
    titles_to_use = None
    
    if request.video_ids:
        # Construct full youtube URLs for each ID
        urls_to_download = [f"https://www.youtube.com/watch?v={vid}" for vid in request.video_ids]
        if request.video_titles and len(request.video_titles) == len(urls_to_download):
            titles_to_use = request.video_titles
    else:
        # Single video url
        urls_to_download = [url]
        if request.video_titles and len(request.video_titles) == 1:
            titles_to_use = request.video_titles
        
    if not urls_to_download:
        raise HTTPException(status_code=400, detail="No videos selected for download.")
        
    task_id = str(uuid.uuid4())
    tasks[task_id] = {
        "status": "pending",
        "queue": asyncio.Queue(),
        "final_file_path": None,
        "display_filename": None,
        "error": None,
        "created_at": time.time()
    }
    
    background_tasks.add_task(run_download_task, task_id, urls_to_download, request.format, titles_to_use)
    
    return {"task_id": task_id}

@app.get("/api/download/progress/{task_id}")
async def get_progress_sse_endpoint(task_id: str):
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="Task not found")
        
    async def event_generator():
        queue = tasks[task_id]["queue"]
        while True:
            try:
                msg = await queue.get()
                yield f"data: {json.dumps(msg)}\n\n"
                
                # Exit the stream once the task terminates
                if msg.get("status") in ["completed", "failed"]:
                    break
            except asyncio.CancelledError:
                # Occurs if client closes the tab or terminates SSE connection
                break
                
    return StreamingResponse(event_generator(), media_type="text/event-stream")

def remove_file(path: str):
    """Helper to remove file from disk after it has been sent to client."""
    try:
        if os.path.exists(path):
            os.remove(path)
            print(f"Cleanup: Removed file after download: {path}")
    except Exception as e:
        print(f"Cleanup error for file {path}: {e}")

@app.get("/api/download/file/{task_id}")
async def get_file_endpoint(task_id: str, background_tasks: BackgroundTasks):
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="Task not found or expired.")
        
    task = tasks[task_id]
    if task["status"] != "completed":
        raise HTTPException(status_code=400, detail=f"Task is in state '{task['status']}' and not completed.")
        
    file_path = task["final_file_path"]
    display_name = task["display_filename"]
    
    if not file_path or not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found on disk.")
        
    # Queue up deletion of the file once served
    background_tasks.add_task(remove_file, file_path)
    
    # Queue up deletion of the task from memory
    background_tasks.add_task(tasks.pop, task_id, None)
    
    return FileResponse(
        path=file_path,
        filename=display_name,
        media_type="application/octet-stream"
    )

# Serve the Single Page App (SPA)
@app.get("/", response_class=HTMLResponse)
async def get_index():
    # We will read templates/index.html and return it
    templates_path = os.path.join(os.path.dirname(__file__), "templates", "index.html")
    if not os.path.exists(templates_path):
        raise HTTPException(status_code=404, detail="Template index.html not found. Please compile the frontend.")
        
    with open(templates_path, "r", encoding="utf-8") as f:
        html_content = f.read()
    return html_content
