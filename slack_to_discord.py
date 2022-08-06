import argparse
import contextlib
import glob
import html
import io
import json
import os
import re
import tempfile
import urllib
import zipfile
from datetime import datetime
from urllib.parse import urlparse

import discord
from discord.errors import Forbidden
from discord.channel import TextChannel

# Restrictions of discord
MAX_MESSAGE_SIZE = 1800  # actually max size = 2000, but there are technical stuff in our messages (username and date)
THREAD_NAME_MAX_NSYMBOLS = 100
THREAD_NAME_MAX_NWORDS = 10  # we split to N words. After that we use slice of first 100 symbols just in case

# Date and time formats
DATE_FORMAT = "%Y-%m-%d"
TIME_FORMAT = "%H:%M"

# Formatting options for messages
# THREAD_FORMAT = ">>>> {date} {time} <**{username}**> {text}"
THREAD_FORMAT = "{date} {time} <**{username}**> {text}"
MSG_FORMAT = "{time} <**{username}**> {text}"
ATTACHMENT_TITLE_TEXT = "<*uploaded a file*> {title}"
ATTACHMENT_ERROR_APPEND = "\n<file thumbnail used due to size restrictions. See original at <{url}>>"

# Create a separator between dates? (None for no)
DATE_SEPARATOR = "{:-^50}"

MENTION_RE = re.compile(r"<([@!#])([^>]*?)(?:\|([^>]*?))?>")
LINK_RE = re.compile(r"<((?:https?|mailto|tel):[A-Za-z0-9_\+\.\-\/\?\,\=\#\:\@\(\)]+)\|([^>]+)>")
EMOJI_RE = re.compile(r":([^ /<>:]+):(?::skin-tone-(\d):)?")


# Map Slack emojis to Discord's versions
# Note that dashes will have been converted to underscores before this is processed
GLOBAL_EMOJI_MAP = {
    "thumbsup_all": "thumbsup",
    "facepunch": "punch",
    "the_horns": "sign_of_the_horns",
    "simple_smile": "slightly_smiling_face",
    "clinking_glasses": "champagne_glass",
    "tornado": "cloud_with_tornado",
    "car": "red_car",
    "us": "flag_us",
    "snow_cloud": "cloud_with_snow",
    "snowman": "snowman2",
    "snowman_without_snow": "snowman",
    "crossed_fingers": "fingers_crossed",
    "hocho": "knife",
    "waving_black_flag": "flag_black",
    "waving_white_flag": "flag_white",
    "woman_heart_man": "couple_with_heart_woman_man",
    "man_heart_man": "couple_with_heart_mm",
    "woman_heart_woman": "couple_with_heart_ww",
    "man_kiss_man": "couplekiss_mm",
    "woman_kiss_woman": "couplekiss_ww",
}


def emoji_replace(s, emoji_map):
    def replace(match):
        e, t = match.groups()

        # Emojis in the emoji_map already have bounding :'s and can't have skin
        # tones applied to them so just directly return them.
        if e in emoji_map:
            return emoji_map[e]

        # Convert -'s to "_"s except the 1st char (ex. :-1:)
        # On Slack some emojis use underscores and some use dashes
        # On Discord everything uses underscores
        if len(e) > 1 and "-" in e[1:]:
            e = e[0] + e[1:].replace("-", "_")

        if e in GLOBAL_EMOJI_MAP:
            e = GLOBAL_EMOJI_MAP[e]

        # Convert Slack's skin tone system to Discord's
        if t is not None:
            return ":{}_tone{}:".format(e, int(t)-1)
        else:
            return ":{}:".format(e)

    return EMOJI_RE.sub(replace, s)


def slack_usermap(d):
    with open(os.path.join(d, "users.json"), 'rb') as fp:
        data = json.load(fp)
    r = dict()
    for x in data:
        r[x["id"]] = x.get('real_name', x['name'])

    r["USLACKBOT"] = "Slackbot"
    r["B01"] = "Slackbot"
    return r


def slack_channels(d):
    with open(os.path.join(d, "channels.json"), 'rb') as fp:
        data = json.load(fp)

    topic = lambda x: "\n\n".join([x[k]["value"] for k in ("purpose", "topic") if x[k]["value"]])
    pins = lambda x: set(p["id"] for p in x.get("pins", []))

    # TODO: verify this works
    # (this is a guess based on API docs since I couldn't get a private data export from Slack)
    is_private = lambda x: x.get("is_private", False)

    out = {x["name"]: (topic(x), is_private(x), pins(x)) for x in data}
    return out


def slack_filedata(f):
    # Make sure the filename has the correct extension
    # Not fixing these issues can cause pictures to not be shown
    # if "filetype" not in f:
    #     a = 1
    ft = f["filetype"]
    name, *ext = f["name"].rsplit(".", 1)
    if not name:
        name = "unknown"

    ext = ext[0] if ext else None
    if not ext:
        # Add missing extension
        ext = ft
    elif ext.lower() != ft.lower():
        # Fix incorrect extension
        name = "{}.{}".format(name, ext)
        ext = ft

    # Make a list of thumbnails for this file in case the original can't be posted
    thumbs = [f[t] for t in sorted((k for k in f if re.fullmatch("thumb_(\d+)", k)), key=lambda x: int(x.split("_")[-1]), reverse=True)]
    if "thumb_video" in f:
        thumbs.append(f["thumb_video"])

    return {
        "name": "{}.{}".format(name, ext),
        "title": f["title"],
        "url": f["url_private"],
        "thumbs": thumbs
    }


def slack_channel_messages(d, channel_name, emoji_map, pins):
    users = slack_usermap(d)

    def mention_repl(m):
        type_ = m.group(1)
        target = m.group(2)
        channel_name = m.group(3)

        if type_ == "#":
            return "`#{}`".format(channel_name)
        elif channel_name is not None:
            return m.group(0)

        if type_ == "@":
            return "`@{}`".format(users.get(target, "[unknown]"))
        elif type_ == "!":
            return "`@{}`".format(target)
        return m.group(0)

    messages = {}
    file_ts_map = {}
    for file in sorted(glob.glob(os.path.join(d, channel_name, "*.json"))):
        with open(file, 'rb',) as fp:
            data = json.load(fp)
        for d in sorted(data, key=lambda x: x["ts"]):
            text = d["text"]
            text = MENTION_RE.sub(mention_repl, text)
            text = LINK_RE.sub(lambda x: x.group(1), text)
            text = emoji_replace(text, emoji_map)
            text = html.unescape(text)
            text = text.rstrip()

            ts = d["ts"]

            user_id = d.get("user")
            subtype = d.get("subtype", "")
            files = d.get("files", [])
            thread_ts = d.get("thread_ts", ts)
            events = {}

            # add bots to user map as they're discovered
            if subtype.startswith("bot_") and "bot_id" in d and d["bot_id"] not in users:
                users[d["bot_id"]] = d.get("username", "[unknown bot]")
                user_id = d["bot_id"]

            # Treat file comments as threads started on the message that posted the file
            elif subtype == "file_comment":
                text = d["comment"]["comment"]
                user_id = d["comment"]["user"]
                file_id = d["file"]["id"]
                thread_ts = file_ts_map.get(file_id, ts)
                # remove the commented file from this messages's files
                files = [x for x in files if x["id"] != file_id]

            # Handle "/me <text>" commands (italicize)
            elif subtype == "me_message":
                text = "*{}*".format(text)

            elif subtype == "reminder_add":
                text = "<*{}*>".format(text.strip())

            # Handle channel operations
            elif subtype == "channel_join":
                text = "<*joined the channel*>"
            elif subtype == "channel_leave":
                text = "<*left the channel*>"
            elif subtype == "channel_archive":
                text = "<*archived the channel*>"

            # Handle setting channel topic/purpose
            elif subtype == "channel_topic" or subtype == "channel_purpose":
                events["topic"] = d.get("topic", d.get("purpose"))
                if events["topic"]:
                    text = "<*set the channel topic*>: {}".format(events["topic"])
                else:
                    text = "<*cleared the channel topic*>"

            if ts in pins:
                events["pin"] = True

            # Store a map of fileid to ts so file comments can be treated as replies
            for f in files:
                file_ts_map[f["id"]] = ts

            dt = datetime.fromtimestamp(float(ts))
            msg = {
                "username": users.get(user_id, "[unknown]"),
                "datetime": dt,
                "time": dt.strftime(TIME_FORMAT),
                "date": dt.strftime(DATE_FORMAT),
                "text": text,
                "replies": {},
                "reactions": {
                    emoji_replace(":{}:".format(x["name"]), emoji_map): [
                        users.get(u, "[unknown]").replace("_", "\\_")
                        for u in x["users"]
                    ]
                    for x in d.get("reactions", [])
                },
                "files": [slack_filedata(f) for f in files if "filetype" in f],
                "events": events
            }

            # If this is a reply, add it to the parent message's replies
            # Replies have a "thread_ts" that differs from their "ts"
            if thread_ts != ts:
                if thread_ts not in messages:
                    # Orphan thread message - skip it
                    continue
                messages[thread_ts]["replies"][ts] = msg
            else:
                messages[ts] = msg

    # Sort the dicts by timestamp and yield the messages
    for msg in (messages[x] for x in sorted(messages.keys())):
        msg["replies"] = [msg["replies"][x] for x in sorted(msg["replies"].keys())]
        yield msg


def split_message(full_text: str):
    nchunks = len(full_text) // MAX_MESSAGE_SIZE + 1
    text_chunks = [full_text[i * MAX_MESSAGE_SIZE: (i + 1) * MAX_MESSAGE_SIZE] for i in range(nchunks)]
    if len(text_chunks[-1]) == 0:
        text_chunks = text_chunks[:-1]
    return text_chunks


def make_discord_msgs(msg: dict, is_reply):
    msg_fmt = (THREAD_FORMAT if is_reply else MSG_FORMAT)
    
    # Split long message and 
    full_text = msg.get("text")
    msg_len = len(full_text)
    if msg_len > MAX_MESSAGE_SIZE:
        text_chunks = split_message(full_text)

        # Send first chunk with date and username
        sub_msg = msg.copy()
        sub_msg["text"] = text_chunks[0]
        yield {"content": msg_fmt.format(**sub_msg)}

        # Send other chunks without date and username except last chunk
        for text_chunk in text_chunks[1:-1]:
            yield {"content": text_chunk}

        # further code will process only last chunk, 
        # i.e. attachments and reactions will be attibuted to last message chunk
        msg["text"] = text_chunks[-1]

    # Show reactions listed in an embed
    embed = None
    if msg["reactions"]:
        embed = discord.Embed(
            description="\n".join(
                "{} {}".format(k, ", ".join(v)) for k, v in msg["reactions"].items()
            )
        )

    # Send the original message without any files
    if len(msg["files"]) == 1:
        # if there is a single file attached, put reactions on the the file
        if msg.get("text"):
            yield {
                "content": msg_fmt.format(**msg),
            }
    elif msg.get("text") or embed:
        # for no/multiple files, put reactions on the message (even if blank)
        yield {
            "content": msg_fmt.format(**msg),
            "embed": embed,
        }
        embed = None

    # Send one messge per image that was posted (using the picture title as the message)
    for f in msg["files"]:
        yield {
            "content": msg_fmt.format(**{**msg, "text": ATTACHMENT_TITLE_TEXT.format(**f)}),
            "file_data": f,
            "embed": embed
        }
        embed = None


def file_upload_attempts(data):
    # Files that are too big cause issues
    # yield data to try to send (original, then thumbnails)
    fd = data.pop("file_data", None)
    if not fd:
        yield data
        return

    for i, url in enumerate([fd["url"]] + fd.get("thumbs", [])):
        if i > 0:
            # Trying thumbnails - get the filename from Slack (it has the correct extension)
            filename = urlparse(url).path.rsplit("/", 1)[-1]
        else:
            filename = fd["name"]

        try:
            f = discord.File(
                fp=io.BytesIO(urllib.request.urlopen(url).read()),
                filename=filename
            )
        except Exception:
            pass
        else:
            yield {
                **data,
                "file": f
            }

        # The original URL failed - trying thumbnails
        if i < 1:
            data["content"] += ATTACHMENT_ERROR_APPEND.format(**fd)

    print("Failed to upload file for message '{}'".format(data["content"]))

    # Just post the message without the attachment
    yield data


class MyClient(discord.Client):

    def __init__(self, *args, data_dir, guild_name, all_private, skip_existing_channels, start, end, **kwargs):
        self._data_dir = data_dir
        self._guild_name = guild_name
        self._prev_msg = None
        self._all_private = all_private
        self._skip_existing_channels = skip_existing_channels,
        self._start, self._end = [datetime.strptime(x, DATE_FORMAT).date() if x else None for x in (start, end)]

        self._started = False # TODO: async equiv of a threading.event
        super().__init__(*args, **kwargs)

    async def on_ready(self):
        if self._started:
            return

        print("Done!")
        try:
            g = discord.utils.get(self.guilds, name=self._guild_name)
            if g is None:
                print("Guild {} not accessible to bot".format(self._guild_name))
                print("Available guilds:\n{}\n".format(self.guilds))
                return

            await self._run_import(g)
        finally:
            print("Bot logging out")
            await self.close()


    async def _send_slack_msg(self, channel: TextChannel, msg, thread=None):
        is_reply = bool(thread)

        if not is_reply and DATE_SEPARATOR:
            msg_date = msg["date"]
            if (
                not self._prev_msg or
                self._prev_msg["date"] != msg_date
            ):
                await channel.send(content=DATE_SEPARATOR.format(msg_date))
            self._prev_msg = msg

        message_obj = None
        pin = msg["events"].pop("pin", False)
        for data in make_discord_msgs(msg, is_reply):
            for attempt in file_upload_attempts(data):
                with contextlib.suppress(Exception):
                    if is_reply:
                        message_obj = await thread.send(**attempt)
                    else:
                        message_obj = await channel.send(**attempt)
                    if pin:
                        pin = False
                        # Requires the "manage messages" optional permission
                        with contextlib.suppress(Forbidden):
                            await message_obj.pin()
                    break
            else:
                print("Failed to post message: '{}'\n".format(data["content"]))
        if is_reply:
            message_obj = None

        return message_obj

    async def _run_import(self, g):
        self._started = True
        emoji_map = {x.name: str(x) for x in self.emojis}

        print("Importing messages...")
        c_chan, c_msg, start_time = 0, 0, datetime.now()

        existing_channels = {x.name: x for x in g.text_channels}

        for c, (init_topic, is_private, pins) in slack_channels(self._data_dir).items():
            if self._skip_existing_channels and c in existing_channels:
                print("Pass existing channel '{}'".format(c))
                continue 

            init_topic = emoji_replace(init_topic, emoji_map)
            ch = None

            print("Processing channel {}...".format(c))
            print("Sending messages...")

            for msg in slack_channel_messages(self._data_dir, c, emoji_map, pins):
                # skip messages that are too early, stop when messages are too late
                if self._end and msg["datetime"].date() > self._end:
                    break
                elif self._start and msg["datetime"].date() < self._start:
                    continue

                # Now that we have a message to send, get/create the channel to send it to
                if ch is None:
                    if c not in existing_channels:
                        if self._all_private or is_private:
                            print("Creating private channel")
                            overwrites = {
                                g.default_role: discord.PermissionOverwrite(read_messages=False),
                                g.me: discord.PermissionOverwrite(read_messages=True),
                            }
                            ch = await g.create_text_channel(c, topic=init_topic, overwrites=overwrites)
                        else:
                            print("Creating public channel")
                            ch = await g.create_text_channel(c, topic=init_topic)
                    else:
                        ch = existing_channels[c]
                    c_chan += 1

                topic = msg["events"].get("topic", None)
                if topic is not None and topic != ch.topic:
                    # Note that the ratelimit is pretty extreme for this
                    # (2 edits per 10 minutes) so it may take a while if there
                    # a lot of topic changes
                    await ch.edit(topic=topic)

                # Send message and threaded replies
                message_obj = await self._send_slack_msg(ch, msg)
                c_msg += 1
                if len(msg["replies"]) and message_obj is not None:
                    tname = " ".join(msg["text"].split()[:THREAD_NAME_MAX_NWORDS])[:THREAD_NAME_MAX_NSYMBOLS]
                    tname = tname if len(tname) else "Thread"  # if thread created for image-message that absent text discord.py cannot create thread
                    thrd = await message_obj.create_thread(name=tname)
                    for rmsg in msg["replies"]:
                        await self._send_slack_msg(ch, rmsg, thread=thrd)
                        c_msg += 1
            print("Done!")
        print("Imported {} messages into {} channel(s) in {}".format(c_msg, c_chan, datetime.now()-start_time))


def main():
    infile = "./MitoFunGen01 Slack export Jun 26 2021 - Aug 5 2022.zip"
    guild = "mitoclub"
    with open("./token.txt") as fin:
        token = fin.read().strip()

    print("Extracting zipfile...", end="", flush=True)
    with tempfile.TemporaryDirectory() as t:
        with zipfile.ZipFile(infile, 'r') as z:
            z.extractall(t)
        print("Done!")

        print("Logging the bot into Discord...", end="", flush=True)
        client = MyClient(
            data_dir=t,
            guild_name=guild,
            all_private=False,
            skip_existing_channels=True,
            start=None,
            end=None,
        )
        client.run(token)


# def main():
#     parser = argparse.ArgumentParser(
#         description="Import Slack chat history into Discord"
#     )
#     parser.add_argument("-z", "--zipfile", help="The Slack export zip file", required=True)
#     parser.add_argument("-g", "--guild", help="The Discord Guild to import history into", required=True)
#     parser.add_argument("-t", "--token", help="The Discord bot token", required=True)
#     parser.add_argument("-s", "--start", help="The date to start importing from", required=False, default=None)
#     parser.add_argument("-e", "--end", help="The date to end importing at", required=False, default=None)
#     parser.add_argument("-p", "--all-private", help="Import all channels as private channels in Discord", action="store_true", default=False)
#     parser.add_argument("-x", "--skip-existing", help="Skip channel if guild already contain channel with same name", action="store_true", default=False)

#     args = parser.parse_args()

#     print("Extracting zipfile...", end="", flush=True)
#     with tempfile.TemporaryDirectory() as t:
#         with zipfile.ZipFile(args.zipfile, 'r') as z:
#             z.extractall(t)
#         print("Done!")

#         print("Logging the bot into Discord...", end="", flush=True)
#         client = MyClient(
            # data_dir=t,
#             guild_name=args.guild,
#             all_private=args.all_private,
#             skip_existing_channel=args.skip_existing
#             start=args.start,
#             end=args.end
#         )
#         client.run(args.token)


if __name__ == "__main__":
    main()
