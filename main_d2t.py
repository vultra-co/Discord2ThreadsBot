# /home/vulture/d2t/main_d2t.py

import discord
import requests
import os
import json
import asyncio
import time
import logging
from dotenv import load_dotenv
from threading import Thread, Lock
from collections import deque
from functools import partial
from flask import Flask, jsonify

# --- Basic Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] [D2Threads] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging.getLogger('discord.gateway').setLevel(logging.WARNING)
logging.getLogger('discord.client').setLevel(logging.WARNING)
logging.getLogger('discord.http').setLevel(logging.WARNING)
logging.getLogger('werkzeug').setLevel(logging.WARNING)

# --- Configuration ---
load_dotenv()
DISCORD_BOT_TOKEN_THREADS = os.getenv('DISCORD_BOT_TOKEN_THREADS')
TARGET_DISCORD_CHANNEL_ID_THREADS_STR = os.getenv('TARGET_DISCORD_CHANNEL_ID_THREADS')
THREADS_USER_ACCESS_TOKEN = os.getenv('THREADS_USER_ACCESS_TOKEN')
THREADS_USER_ID = os.getenv('THREADS_USER_ID')
THREADS_BOT_API_PORT = int(os.getenv('THREADS_BOT_API_PORT', '8081'))

if not all([DISCORD_BOT_TOKEN_THREADS, TARGET_DISCORD_CHANNEL_ID_THREADS_STR, THREADS_USER_ACCESS_TOKEN, THREADS_USER_ID]):
    logging.critical("[D2Threads] FATAL ERROR: Missing critical environment variables. Check .env file.")
    exit(1)
try:
    TARGET_DISCORD_CHANNEL_ID_THREADS = int(TARGET_DISCORD_CHANNEL_ID_THREADS_STR)
except ValueError:
    logging.critical(f"[D2Threads] FATAL ERROR: TARGET_DISCORD_CHANNEL_ID_THREADS ('{TARGET_DISCORD_CHANNEL_ID_THREADS_STR}') is not valid.")
    exit(1)

THREADS_API_VERSION = "v1.0"
THREADS_GRAPH_URL = f"https://graph.threads.net/{THREADS_API_VERSION}"
STATE_FILE_THREADS = 'discord_to_threads_state.json'

# --- Global State Variables & Lock (for Threads Bot) ---
state_lock_threads = Lock()
discord_to_threads_map = {}
posts_attempted_threads = 0
posts_succeeded_threads = 0
posts_failed_threads = 0
discord_status_threads = "Initializing"
last_threads_api_status = "Unknown"
last_threads_api_timestamp = None
recent_activity_threads = deque(maxlen=10)
pending_post_events_threads = {}

# --- State Persistence ---
def load_threads_state():
    global discord_to_threads_map, posts_attempted_threads, posts_succeeded_threads, posts_failed_threads
    with state_lock_threads:
        try:
            with open(STATE_FILE_THREADS, 'r') as f:
                data = json.load(f)
                discord_to_threads_map = {k: v for k, v in data.get("discord_to_threads_map", {}).items() if v != "PENDING_POST"}
                posts_attempted_threads = data.get("posts_attempted_threads", 0)
                posts_succeeded_threads = data.get("posts_succeeded_threads", 0)
                posts_failed_threads = data.get("posts_failed_threads", 0)
                logging.info(f"Loaded Threads state: {len(discord_to_threads_map)} mappings from {STATE_FILE_THREADS}.")
        except FileNotFoundError:
            logging.warning(f"{STATE_FILE_THREADS} not found, starting with empty mappings.")
            discord_to_threads_map = {}
            posts_attempted_threads = posts_succeeded_threads = posts_failed_threads = 0
        except Exception as e:
            logging.error(f"Error loading {STATE_FILE_THREADS}: {e}", exc_info=True)
            discord_to_threads_map = {}
            posts_attempted_threads = posts_succeeded_threads = posts_failed_threads = 0
        if not isinstance(discord_to_threads_map, dict):
            discord_to_threads_map = {}

def save_threads_state():
    with state_lock_threads:
        try:
            savable_map = {k: v for k, v in discord_to_threads_map.items() if v != "PENDING_POST"}
            data_to_save = {
                "discord_to_threads_map": savable_map,
                "posts_attempted_threads": posts_attempted_threads,
                "posts_succeeded_threads": posts_succeeded_threads,
                "posts_failed_threads": posts_failed_threads
            }
            with open(STATE_FILE_THREADS, 'w') as f:
                json.dump(data_to_save, f, indent=4)
            logging.debug(f"Saved Threads state to {STATE_FILE_THREADS}.")
        except Exception as e:
            logging.error(f"Error saving {STATE_FILE_THREADS}: {e}", exc_info=True)

def update_mapping_status(discord_msg_id, threads_post_id_or_status):
    discord_msg_id_str = str(discord_msg_id)
    with state_lock_threads:
        discord_to_threads_map[discord_msg_id_str] = str(threads_post_id_or_status)
    if threads_post_id_or_status != "PENDING_POST":
        save_threads_state()
    else:
        logging.info(f"[D2Threads] Marked Discord Msg ID {discord_msg_id_str} as PENDING_POST.")

def clear_mapping_entry(discord_msg_id):
    discord_msg_id_str = str(discord_msg_id)
    with state_lock_threads:
        if discord_msg_id_str in discord_to_threads_map:
            del discord_to_threads_map[discord_msg_id_str]
    save_threads_state()

def get_threads_post_id_or_status(discord_msg_id):
    with state_lock_threads:
        return discord_to_threads_map.get(str(discord_msg_id))

def add_threads_activity(activity_type, status, details=""):
    with state_lock_threads:
        try:
            log_entry = {
                "timestamp": time.time(), "type": str(activity_type),
                "status": str(status), "details": str(details)
            }
            recent_activity_threads.appendleft(log_entry)
        except Exception as e:
            logging.error(f"Error adding Threads Bot activity: {e}", exc_info=True)

# --- Threads API Interaction Functions ---
def _make_threads_api_request_sync(method, endpoint, params=None, json_data=None, is_publishing_container=False) -> dict | None:
    global last_threads_api_status, last_threads_api_timestamp, posts_attempted_threads, posts_succeeded_threads, posts_failed_threads
    url = f"{THREADS_GRAPH_URL}{endpoint}"
    all_params = {'access_token': THREADS_USER_ACCESS_TOKEN}
    if params: all_params.update(params)
    
    is_content_creation_attempt = method.upper() == 'POST' and not is_publishing_container and \
                                  (endpoint.endswith("/threads") or "/replies" in endpoint)
    if is_content_creation_attempt:
        with state_lock_threads: posts_attempted_threads += 1
    
    current_time = time.time()
    try:
        log_params = {k: v for k, v in all_params.items() if k != 'access_token'}
        logging.info(f"Threads API Request: {method} {url} with query_params: {log_params} (JSON Data: {json_data})")
        if method.upper() == 'POST': response = requests.post(url, params=all_params, json=json_data, timeout=60)
        elif method.upper() == 'GET': response = requests.get(url, params=all_params, timeout=30)
        else: logging.error(f"Unsupported HTTP method: {method}"); return None
        logging.info(f"Threads API Response Status: {response.status_code}")
        
        if not (response.status_code >= 200 and response.status_code < 300):
            logging.warning(f"Threads API Non-2xx Response Text: {response.text[:500]}")

        if response.status_code >= 200 and response.status_code < 300:
            # For successful POSTs that create content or publish, update status
            if method.upper() == 'POST': # Includes container creation, publish, and reply
                 with state_lock_threads: 
                    last_threads_api_status = "✅ API Call Success (POST)"
                    last_threads_api_timestamp = current_time
                    # posts_succeeded_threads is incremented after successful publish_threads_container or publish_threads_reply
            if response.text and len(response.content) > 0:
                try: return response.json()
                except json.JSONDecodeError:
                    logging.error(f"Failed to decode JSON: {response.text[:200]}")
                    if method.upper() == 'GET' and not is_publishing_container :
                         return {"status_code_from_text": response.status_code, "raw_text": response.text, "status": "UNKNOWN_NON_JSON"}
                    return {"error": {"message": "Invalid JSON response"}}
            return {"status": "success_no_content"} if is_publishing_container else {}
        
        error_payload = {"message": f"API Error {response.status_code}"}
        try: 
            error_data = response.json()
            error_payload = error_data.get("error", error_payload)
            for key in ["type", "code", "error_subcode", "fbtrace_id", "error_user_title", "error_user_msg"]:
                if key in error_data.get("error", {}): error_payload[key] = error_data["error"][key]
                elif key in error_data: error_payload[key] = error_data[key]
        except json.JSONDecodeError: error_payload["details"] = response.text[:200]
        logging.error(f"Threads API Error: {response.status_code} - {error_payload}")
        with state_lock_threads: 
            last_threads_api_status = f"❌ API Error {response.status_code}"
            last_threads_api_timestamp = current_time
            if is_content_creation_attempt: posts_failed_threads +=1
        save_threads_state()
        return {"error": error_payload}
    except requests.exceptions.RequestException as e:
        logging.error(f"Request Exception: {e}", exc_info=True)
        with state_lock_threads: 
            last_threads_api_status = "❌ Request Exception"
            last_threads_api_timestamp = current_time
            if is_content_creation_attempt: posts_failed_threads +=1
        save_threads_state()
        return {"error": {"message": f"Request Exception: {e}"}}
    except Exception as e:
        logging.error(f"Unexpected API error: {e}", exc_info=True)
        with state_lock_threads: 
            last_threads_api_status = "❌ Unexpected API Error"
            last_threads_api_timestamp = current_time
            if is_content_creation_attempt: posts_failed_threads +=1
        save_threads_state()
        return {"error": {"message": f"Unexpected Error: {e}"}}

async def create_threads_text_container(text_content: str, reply_to_id: str | None = None) -> str | None:
    endpoint = f"/{THREADS_USER_ID}/threads"
    params = {'media_type': 'TEXT', 'text': text_content}
    if reply_to_id: params['reply_to_id'] = reply_to_id
    logging.info(f"Creating text container (reply_to: {reply_to_id}).")
    loop = asyncio.get_event_loop()
    func_to_run = partial(_make_threads_api_request_sync, method='POST', endpoint=endpoint, params=params)
    response_data = await loop.run_in_executor(None, func_to_run)
    if response_data and not response_data.get("error") and response_data.get('id'):
        return response_data['id']
    logging.error(f"Failed to create Threads text container, response: {response_data}")
    return None

async def create_threads_image_container(image_url: str, text_caption: str | None = None, reply_to_id: str | None = None) -> str | None:
    endpoint = f"/{THREADS_USER_ID}/threads"
    params = {'media_type': 'IMAGE', 'image_url': image_url}
    if text_caption: params['text'] = text_caption
    if reply_to_id: params['reply_to_id'] = reply_to_id
    logging.info(f"Creating image container (reply_to: {reply_to_id}) for URL: {image_url}")
    loop = asyncio.get_event_loop()
    func_to_run = partial(_make_threads_api_request_sync, method='POST', endpoint=endpoint, params=params)
    response_data = await loop.run_in_executor(None, func_to_run)
    if response_data and not response_data.get("error") and response_data.get('id'):
        return response_data['id']
    logging.error(f"Failed to create image container, response: {response_data}")
    return None

async def create_threads_video_container(video_url: str, text_caption: str | None = None, reply_to_id: str | None = None) -> str | None:
    endpoint = f"/{THREADS_USER_ID}/threads"
    params = {'media_type': 'VIDEO', 'video_url': video_url}
    if text_caption: params['text'] = text_caption
    if reply_to_id: params['reply_to_id'] = reply_to_id
    logging.info(f"Creating video container (reply_to: {reply_to_id}) for URL: {video_url}")
    loop = asyncio.get_event_loop()
    func_to_run = partial(_make_threads_api_request_sync, method='POST', endpoint=endpoint, params=params)
    response_data = await loop.run_in_executor(None, func_to_run)
    if response_data and not response_data.get("error") and response_data.get('id'):
        return response_data['id']
    logging.error(f"Failed to create video container, response: {response_data}")
    return None

async def get_threads_container_status(container_id: str) -> str | None:
    endpoint = f"/{container_id}"
    params_status_only = {'fields': 'status'}
    loop = asyncio.get_event_loop()
    logging.info(f"Getting status for container {container_id} with fields=status")
    func_to_run = partial(_make_threads_api_request_sync, method='GET', endpoint=endpoint, params=params_status_only)
    response_data = await loop.run_in_executor(None, func_to_run)
    logging.debug(f"Raw status response for container {container_id}: {response_data}")
    if response_data and not response_data.get("error"):
        if response_data.get('status'):
            status_value = response_data['status']
            logging.info(f"Container {container_id} status: {status_value}")
            if isinstance(status_value, str): return status_value.upper()
        elif response_data.get('status_code'): # Fallback
            status_code_value = response_data['status_code']
            logging.info(f"Container {container_id} status_code: {status_code_value}")
            return status_code_value
    logging.error(f"Failed to get clear status for container {container_id}. Response: {response_data}")
    return None

async def publish_threads_container(container_id: str, media_type_hint: str = "UNKNOWN") -> str | None:
    global posts_succeeded_threads, posts_failed_threads # To update counters
    if media_type_hint == "VIDEO": max_retries, retry_delay = 12, 15
    else: max_retries, retry_delay = 5, 6
    logging.info(f"Polling for {media_type_hint} container {container_id} (max {max_retries} retries)...")
    for i in range(max_retries):
        status = await get_threads_container_status(container_id)
        if status == 'FINISHED': logging.info(f"Container {container_id} FINISHED."); break
        if status in ['ERROR', 'EXPIRED'] or status is None:
            logging.error(f"Container {container_id} state '{status}' or status check failed. Cannot publish.")
            with state_lock_threads: posts_failed_threads += 1 # Count as failed if container errored
            save_threads_state()
            return None
        if i < max_retries - 1:
            logging.info(f"Container {container_id} (Status: {status}). Retrying in {retry_delay}s... ({i+1}/{max_retries})")
            await asyncio.sleep(retry_delay)
        else:
            logging.error(f"Container {container_id} not FINISHED after {max_retries} retries. Last: {status}.")
            if media_type_hint == "VIDEO":
                 logging.error(f"VIDEO container {container_id} did not finish. Aborting publish.")
                 with state_lock_threads: posts_failed_threads += 1
                 save_threads_state()
                 return None
            logging.warning(f"Attempting publish for non-video container {container_id} despite non-FINISHED status.")
            break 
    endpoint = f"/{THREADS_USER_ID}/threads_publish"
    params = {'creation_id': container_id}
    loop = asyncio.get_event_loop()
    func_to_run = partial(_make_threads_api_request_sync, method='POST', endpoint=endpoint, params=params, is_publishing_container=True)
    response_data = await loop.run_in_executor(None, func_to_run)
    if response_data and not response_data.get("error") and response_data.get('id'):
        with state_lock_threads: posts_succeeded_threads += 1
        save_threads_state()
        return response_data['id']
    if response_data and response_data.get("status") == "success_no_content":
        logging.warning(f"Container {container_id} published (200 OK), but API no post ID. Using container ID.")
        with state_lock_threads: posts_succeeded_threads += 1 # Count as success if 200 OK
        save_threads_state()
        return container_id 
    logging.error(f"Failed to publish container {container_id}, response: {response_data}")
    # posts_failed_threads was already incremented by _make_threads_api_request_sync if it was a content creation attempt
    # but publishing is a distinct step. If _make_threads_api_request_sync returned error for publish, it's already counted.
    # If it returned None without error (e.g. unsupported method), we might need to count failure here.
    # For now, assuming _make_threads_api_request_sync handles the counter update on its own failure.
    return None

async def send_to_threads(text: str, attachments: list[discord.Attachment], reply_to_threads_post_id: str | None = None):
    original_text_content = text if text else ""
    processed_text_for_post = original_text_content
    media_type_hint = "TEXT"
    container_id = None
    first_valid_attachment = None

    if attachments:
        for att in attachments:
            ct = att.content_type
            if ct and (ct.startswith('image/') or ct.startswith('video/')):
                first_valid_attachment = att
                break
        if len(attachments) > 1 and first_valid_attachment:
            processed_text_for_post += (f"\n\n[Note: {len(attachments)} attachments. Processing first: '{first_valid_attachment.filename}'. Multi-media needs different API calls.]")
        elif not first_valid_attachment and attachments: 
             processed_text_for_post += f"\n\n[Note: {len(attachments)} attachment(s) of unsupported types.]"

    if first_valid_attachment:
        content_type = first_valid_attachment.content_type
        media_url = first_valid_attachment.url
        logging.info(f"[D2Threads] Processing attachment for Threads: {first_valid_attachment.filename} (Type: {content_type})")
        if content_type.startswith('image/'):
            media_type_hint = "IMAGE"
            container_id = await create_threads_image_container(image_url=media_url, text_caption=processed_text_for_post, reply_to_id=reply_to_threads_post_id)
        elif content_type.startswith('video/'):
            media_type_hint = "VIDEO"
            container_id = await create_threads_video_container(video_url=media_url, text_caption=processed_text_for_post, reply_to_id=reply_to_threads_post_id)
    elif processed_text_for_post: 
        media_type_hint = "TEXT"
        container_id = await create_threads_text_container(text_content=processed_text_for_post, reply_to_id=reply_to_threads_post_id)
    else:
        logging.warning("[D2Threads] No text and no processable media. Nothing to create container for.")
        return None

    if container_id:
        logging.info(f"[D2Threads] Container {container_id} (type: {media_type_hint}, reply_to: {reply_to_threads_post_id}) created. Publishing.")
        return await publish_threads_container(container_id, media_type_hint)
    logging.error(f"[D2Threads] Failed to create any container for the message (reply_to: {reply_to_threads_post_id}).")
    return None

# --- Flask App for Threads Bot Status API ---
tg_flask_app = Flask(__name__)
@tg_flask_app.route('/api/status')
def route_threads_bot_api_status():
    with state_lock_threads:
        current_bot_status_threads = "Running ✅"
        if "Disconnected" in discord_status_threads: current_bot_status_threads = "Discord Disconnected ❌"
        elif "Connecting" in discord_status_threads: current_bot_status_threads = "Discord Connecting ⚠️"
        elif "❌" in last_threads_api_status: current_bot_status_threads = "Running with API Errors ⚠️"
        status_data = {
            "bot_name": "Discord-to-Threads Bot", "bot_status": current_bot_status_threads,
            "discord_status": discord_status_threads, "last_target_api_status": last_threads_api_status,
            "last_target_api_timestamp": last_threads_api_timestamp, "posts_attempted": posts_attempted_threads,
            "posts_succeeded": posts_succeeded_threads, "posts_failed": posts_failed_threads,
            "recent_activity": list(recent_activity_threads), "monitoring_channel": TARGET_DISCORD_CHANNEL_ID_THREADS
        }
    return jsonify(status_data)

def run_threads_flask_api():
    try:
        logging.info(f"[D2Threads] Starting Status API Flask server on 0.0.0.0:{THREADS_BOT_API_PORT}")
        tg_flask_app.run(host='0.0.0.0', port=THREADS_BOT_API_PORT, debug=False, use_reloader=False)
    except Exception as e:
        logging.critical(f"[D2Threads] FATAL ERROR: Threads Bot Status API Flask server failed: {e}", exc_info=True)

# --- Discord Client Setup ---
intents_threads = discord.Intents.default()
intents_threads.message_content = True
client_threads = discord.Client(intents=intents_threads)

# --- Discord Event Handlers ---
@client_threads.event
async def on_ready():
    global discord_status_threads
    load_threads_state()
    with state_lock_threads: discord_status_threads = "Connected"
    add_threads_activity("Discord", "✅ Connected", f"Logged in as {client_threads.user.name}")
    logging.info(f'[D2Threads] Logged in as {client_threads.user.name} ({client_threads.user.id})')
    logging.info(f'[D2Threads] Monitoring Channel ID: {TARGET_DISCORD_CHANNEL_ID_THREADS}')
    logging.info('[D2Threads] ------ Threads Bot Ready ------')

@client_threads.event
async def on_disconnect():
    global discord_status_threads
    with state_lock_threads: discord_status_threads = "Disconnected"
    add_threads_activity("Discord", "❌ Disconnected", "Lost gateway connection")
    logging.warning('[D2Threads] Threads Bot disconnected from Discord.')

@client_threads.event
async def on_connect():
    global discord_status_threads
    with state_lock_threads: discord_status_threads = "Connecting"
    logging.info('[D2Threads] Threads Bot connecting to Discord gateway...')

@client_threads.event
async def on_resumed():
    global discord_status_threads
    with state_lock_threads: discord_status_threads = "Connected (Resumed)"
    add_threads_activity("Discord", "✅ Resumed", "Session resumed")
    logging.info('[D2Threads] Threads Bot session resumed.')

@client_threads.event
async def on_message(message: discord.Message):
    logging.info(f"[D2Threads] >>> on_message START - ID:{message.id} Chan:{message.channel.id} Type:{message.type} Thread?:{isinstance(message.channel, discord.Thread)} Content:'{message.content[:30]}...'")
    if message.author == client_threads.user: return
    if message.type == discord.MessageType.thread_created:
        logging.info(f"[D2Threads] Ignoring system message {message.id}: Type is thread_created.")
        return

    is_in_target_channel = message.channel.id == TARGET_DISCORD_CHANNEL_ID_THREADS
    is_in_target_thread = isinstance(message.channel, discord.Thread) and message.channel.parent_id == TARGET_DISCORD_CHANNEL_ID_THREADS
    if not (is_in_target_channel or is_in_target_thread): return

    logging.info(f"[D2Threads] Relevant message: ID {message.id} | In Thread: {is_in_target_thread}")
    threads_post_id_to_reply_to = None 
    is_initial_message_in_channel = False 
    discord_msg_id_str = str(message.id)

    if is_in_target_thread:
        original_discord_message_id_of_thread_starter = str(message.channel.id)
        mapped_status = get_threads_post_id_or_status(original_discord_message_id_of_thread_starter)
        if mapped_status == "PENDING_POST":
            event = pending_post_events_threads.get(original_discord_message_id_of_thread_starter)
            if event:
                logging.info(f"[D2Threads] Original post for {original_discord_message_id_of_thread_starter} PENDING. Awaiting event for reply {message.id}...")
                try:
                    await asyncio.wait_for(event.wait(), timeout=60.0)
                    mapped_status = get_threads_post_id_or_status(original_discord_message_id_of_thread_starter)
                    if mapped_status and mapped_status != "PENDING_POST":
                        threads_post_id_to_reply_to = mapped_status 
                    else: 
                        logging.warning(f"[D2Threads] Original post {original_discord_message_id_of_thread_starter} failed or timed out. Ignoring reply {message.id}.")
                        return
                except asyncio.TimeoutError:
                    logging.warning(f"[D2Threads] Timed out waiting for event for {original_discord_message_id_of_thread_starter}. Ignoring reply {message.id}.")
                    return
            else:
                logging.error(f"[D2Threads] PENDING_POST for {original_discord_message_id_of_thread_starter} but no event. Ignoring reply {message.id}.")
                return
        elif not mapped_status: 
            logging.warning(f"[D2Threads] Original Threads Post ID for {original_discord_message_id_of_thread_starter} not found. Ignoring reply {message.id}.")
            return
        else: 
            threads_post_id_to_reply_to = mapped_status
    elif is_in_target_channel:
        is_initial_message_in_channel = True
        update_mapping_status(discord_msg_id_str, "PENDING_POST")
        pending_post_events_threads[discord_msg_id_str] = asyncio.Event()

    text_content = message.content if message.content else "" 
    attachments = message.attachments
    final_text_to_send = text_content
    has_final_text = bool(final_text_to_send)
    first_valid_attachment_for_note = None
    if attachments:
        for att in attachments:
            ct = att.content_type
            if ct and (ct.startswith('image/') or ct.startswith('video/')):
                first_valid_attachment_for_note = att; break
    if not has_final_text and message.embeds: 
        if message.embeds[0].description:
            final_text_to_send = message.embeds[0].description; has_final_text = True
            logging.info(f"[D2Threads] Using embed description for Msg {message.id}")
        elif not first_valid_attachment_for_note : # No text, no usable embed, no valid attachments
            logging.info(f"[D2Threads] Msg {message.id} no actionable content, skipping.")
            if is_initial_message_in_channel: 
                clear_mapping_entry(discord_msg_id_str) 
                if discord_msg_id_str in pending_post_events_threads: 
                    pending_post_events_threads[discord_msg_id_str].set(); del pending_post_events_threads[discord_msg_id_str]
            return
    if not has_final_text and not first_valid_attachment_for_note: # Still no text and no valid attachments
        logging.info(f"[D2Threads] Msg {message.id} empty after all checks, skipping.")
        if is_initial_message_in_channel: 
            clear_mapping_entry(discord_msg_id_str)
            if discord_msg_id_str in pending_post_events_threads:
                pending_post_events_threads[discord_msg_id_str].set(); del pending_post_events_threads[discord_msg_id_str]
        return

    new_threads_post_id = await send_to_threads(
        text=final_text_to_send, 
        attachments=attachments, 
        reply_to_threads_post_id=threads_post_id_to_reply_to
    )

    if is_initial_message_in_channel:
        if new_threads_post_id:
            update_mapping_status(discord_msg_id_str, new_threads_post_id) 
            logging.info(f"[D2Threads] Saved mapping Discord {discord_msg_id_str} -> Threads {new_threads_post_id}")
        else: 
            clear_mapping_entry(discord_msg_id_str) 
            logging.error(f"[D2Threads] Failed to send initial Discord message {discord_msg_id_str} to Threads.")
        if discord_msg_id_str in pending_post_events_threads:
            pending_post_events_threads[discord_msg_id_str].set()
            del pending_post_events_threads[discord_msg_id_str]
    elif new_threads_post_id and threads_post_id_to_reply_to:
        logging.info(f"[D2Threads] Successfully posted reply. Original: {threads_post_id_to_reply_to}, Reply/Container ID: {new_threads_post_id}")
    elif not new_threads_post_id and threads_post_id_to_reply_to: 
        logging.error(f"[D2Threads] Failed to send Discord reply {message.id} for Threads Post {threads_post_id_to_reply_to}.")
    logging.info(f"[D2Threads] --- Message Handling Complete for Msg ID: {message.id} ---")

async def d2threads_main_async_loop():
    try:
        if client_threads and DISCORD_BOT_TOKEN_THREADS: 
            await client_threads.start(DISCORD_BOT_TOKEN_THREADS)
        else:
            logging.critical("[D2Threads] Discord client or token not initialized.")
    except discord.errors.LoginFailure:
        logging.critical("[D2Threads] FATAL ERROR: Invalid Discord Bot Token.")
    except discord.errors.PrivilegedIntentsRequired:
         logging.critical("[D2Threads] FATAL ERROR: Discord Privileged Intents not enabled.")
    except Exception as e:
        logging.critical(f"[D2Threads] FATAL ERROR in main_async_loop: {e}", exc_info=True)
    finally:
        if client_threads and client_threads.is_ready(): 
            await client_threads.close()
        logging.info("[D2Threads] bot shutdown process completed.")

if __name__ == "__main__":
    load_threads_state() 
    logging.info("Starting Discord-to-Threads Bot...")
    try:
        logging.info("[D2Threads] Starting Status API Flask server...")
        threads_api_flask_thread = Thread(target=run_threads_flask_api, name="ThreadsBotApiFlaskThread")
        threads_api_flask_thread.daemon = True
        threads_api_flask_thread.start()
    except Exception as e:
        logging.critical(f"[D2Threads] Failed to start Status API Flask thread: {e}", exc_info=True)
    time.sleep(1)
    try:
        asyncio.run(d2threads_main_async_loop())
    except KeyboardInterrupt:
        logging.info("[D2Threads] Shutdown requested by user (Ctrl+C).")
    except Exception as e: 
        logging.critical(f"[D2Threads] Application exited unexpectedly: {e}", exc_info=True)

