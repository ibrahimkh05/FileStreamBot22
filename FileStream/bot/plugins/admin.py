import os
import time
import string
import random
import asyncio
import aiofiles
import datetime

from FileStream.utils.broadcast_helper import send_msg
from FileStream.utils.database import Database
from FileStream.bot import FileStream
from FileStream.server.exceptions import FIleNotFound
from FileStream.config import Telegram, Server
from pyrogram import filters, Client
from pyrogram.types import Message
from pyrogram.enums.parse_mode import ParseMode

db = Database(Telegram.DATABASE_URL, Telegram.SESSION_NAME)
broadcast_ids = {}


@FileStream.on_message(filters.command("status") & filters.private & filters.user(Telegram.OWNER_ID))
async def sts(c: Client, m: Message):
    await m.reply_text(text=f"""**Total Users in DB:** `{await db.total_users_count()}`
**Banned Users in DB:** `{await db.total_banned_users_count()}`
**Total Links Generated: ** `{await db.total_files()}`"""
                       , parse_mode=ParseMode.MARKDOWN, quote=True)


@FileStream.on_message(filters.command("ban") & filters.private & filters.user(Telegram.OWNER_ID))
async def ban_user(b, m: Message):
    if len(m.text.split()) < 2:
        await m.reply_text("**Usage:** `/ban user_id`", parse_mode=ParseMode.MARKDOWN, quote=True)
        return
    
    try:
        user_id = int(m.text.split()[1])
    except ValueError:
        await m.reply_text("**Invalid user ID. Please provide a valid number.**", parse_mode=ParseMode.MARKDOWN, quote=True)
        return
    
    if not await db.is_user_banned(user_id):
        try:
            await db.ban_user(user_id)
            await db.delete_user(user_id)
            await m.reply_text(text=f"`{user_id}`** is Banned** ", parse_mode=ParseMode.MARKDOWN, quote=True)
            if not str(user_id).startswith('-100'):
                try:
                    await b.send_message(
                        chat_id=user_id,
                        text="**Your Banned to Use The Bot**",
                        parse_mode=ParseMode.MARKDOWN,
                        disable_web_page_preview=True
                    )
                except Exception as e:
                    print(f"Failed to send ban message to {user_id}: {e}")
        except Exception as e:
            await m.reply_text(text=f"**Something went wrong: {e}** ", parse_mode=ParseMode.MARKDOWN, quote=True)
    else:
        await m.reply_text(text=f"`{user_id}`** is Already Banned** ", parse_mode=ParseMode.MARKDOWN, quote=True)


@FileStream.on_message(filters.command("unban") & filters.private & filters.user(Telegram.OWNER_ID))
async def unban_user(b, m: Message):
    if len(m.text.split()) < 2:
        await m.reply_text("**Usage:** `/unban user_id`", parse_mode=ParseMode.MARKDOWN, quote=True)
        return
    
    try:
        user_id = int(m.text.split()[1])
    except ValueError:
        await m.reply_text("**Invalid user ID. Please provide a valid number.**", parse_mode=ParseMode.MARKDOWN, quote=True)
        return
    
    if await db.is_user_banned(user_id):
        try:
            await db.unban_user(user_id)
            await m.reply_text(text=f"`{user_id}`** is Unbanned** ", parse_mode=ParseMode.MARKDOWN, quote=True)
            if not str(user_id).startswith('-100'):
                try:
                    await b.send_message(
                        chat_id=user_id,
                        text="**You're Unbanned now. You can use The Bot**",
                        parse_mode=ParseMode.MARKDOWN,
                        disable_web_page_preview=True
                    )
                except Exception as e:
                    print(f"Failed to send unban message to {user_id}: {e}")
        except Exception as e:
            await m.reply_text(text=f"**Something went wrong: {e}**", parse_mode=ParseMode.MARKDOWN, quote=True)
    else:
        await m.reply_text(text=f"`{user_id}`** is not Banned** ", parse_mode=ParseMode.MARKDOWN, quote=True)


@FileStream.on_message(filters.command("broadcast") & filters.private & filters.user(Telegram.OWNER_ID) & filters.reply)
async def broadcast_(c, m):
    if not m.reply_to_message:
        await m.reply_text("**Please reply to a message to broadcast it.**")
        return
    
    try:
        all_users = await db.get_all_users()
        broadcast_msg: Message = m.reply_to_message
        broadcast_id = ''.join([random.choice(string.ascii_letters) for _ in range(3)])
        
        while broadcast_ids.get(broadcast_id):
            broadcast_id = ''.join([random.choice(string.ascii_letters) for _ in range(3)])
        
        out = await m.reply_text(
            text="**Broadcast initiated! You will be notified with log file when all the users are notified.**"
        )
        
        start_time = time.time()
        total_users = await db.total_users_count()
        done = 0
        failed = 0
        success = 0
        
        broadcast_ids[broadcast_id] = {
            'total': total_users,
            'current': done,
            'failed': failed,
            'success': success
        }
        
        # Create broadcast log file
        log_file_path = f'broadcast_{broadcast_id}.txt'
        
        async with aiofiles.open(log_file_path, 'w') as broadcast_log_file:
            await broadcast_log_file.write(f"Broadcast started at: {datetime.datetime.now()}\n")
            await broadcast_log_file.write(f"Total users: {total_users}\n\n")
        
        async for user in all_users:
            if not broadcast_ids.get(broadcast_id):
                break
                
            try:
                sts, msg = await send_msg(
                    user_id=int(user['id']),
                    message=broadcast_msg
                )
                
                if msg:
                    async with aiofiles.open(log_file_path, 'a') as broadcast_log_file:
                        await broadcast_log_file.write(msg)
                
                if sts == 200:
                    success += 1
                else:
                    failed += 1
                    
                if sts == 400:
                    await db.delete_user(user['id'])
                    
                done += 1
                
                broadcast_ids[broadcast_id].update({
                    'current': done,
                    'failed': failed,
                    'success': success
                })
                
                # Update status every 50 users
                if done % 50 == 0:
                    try:
                        await out.edit_text(
                            f"**Broadcast Status**\n\n"
                            f"**Total:** {total_users}\n"
                            f"**Completed:** {done}\n"
                            f"**Success:** {success}\n"
                            f"**Failed:** {failed}\n"
                            f"**Remaining:** {total_users - done}"
                        )
                    except Exception as e:
                        print(f"Error updating broadcast status: {e}")
                
                # Small delay to avoid flooding
                await asyncio.sleep(0.1)
                
            except Exception as e:
                failed += 1
                done += 1
                async with aiofiles.open(log_file_path, 'a') as broadcast_log_file:
                    await broadcast_log_file.write(f"Error sending to {user['id']}: {str(e)}\n")
        
        # Final log entry
        async with aiofiles.open(log_file_path, 'a') as broadcast_log_file:
            await broadcast_log_file.write(f"\nBroadcast completed at: {datetime.datetime.now()}\n")
            await broadcast_log_file.write(f"Total: {total_users}, Success: {success}, Failed: {failed}\n")
        
        if broadcast_ids.get(broadcast_id):
            broadcast_ids.pop(broadcast_id)
        
        completed_in = datetime.timedelta(seconds=int(time.time() - start_time))
        
        await asyncio.sleep(3)
        await out.delete()
        
        if failed == 0:
            await m.reply_text(
                text=f"**Broadcast completed in** `{completed_in}`\n\n"
                     f"**Total users:** {total_users}\n"
                     f"**Total done:** {done}\n"
                     f"**Success:** {success}\n" 
                     f"**Failed:** {failed}",
                quote=True
            )
        else:
            await m.reply_document(
                document=log_file_path,
                caption=f"**Broadcast completed in** `{completed_in}`\n\n"
                        f"**Total users:** {total_users}\n"
                        f"**Total done:** {done}\n"
                        f"**Success:**
