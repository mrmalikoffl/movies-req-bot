async def handle_forwarded_message(update, context):
    """Process forwarded message for channel indexing (single or batch)"""
    chat_id = update.message.chat_id

    if update.callback_query and update.callback_query.data == 'index_cancel':
        context.user_data['indexing'] = False
        context.user_data['index_channel_id'] = None
        context.user_data['index_mode'] = None
        await update.callback_query.message.edit_text("Indexing cancelled.")
        logger.info(f"User {chat_id} cancelled indexing")
        return

    if not context.user_data.get('indexing'):
        return

    # Check if user specified indexing mode
    if not context.user_data.get('index_mode'):
        if update.message.text.lower() in ['batch', 'single']:
            context.user_data['index_mode'] = update.message.text.lower()
            await update.message.reply_text(
                f"{context.user_data['index_mode'].capitalize()} indexing selected. "
                "Now forward a message from the channel to index."
            )
            logger.info(f"User {chat_id} selected {context.user_data['index_mode']} indexing")
            return
        else:
            await update.message.reply_text(
                "Please specify 'batch' or 'single' for indexing mode."
            )
            logger.warning(f"User {chat_id} provided invalid indexing mode")
            return

    # Log forwarded message details for debugging
    logger.debug(f"Forwarded message details: "
                f"forward_from_chat={getattr(update.message, 'forward_from_chat', None)}, "
                f"forward_from_message_id={getattr(update.message, 'forward_from_message_id', None)}, "
                f"forward_date={getattr(update.message, 'forward_date', None)}, "
                f"chat_id={update.message.chat.id}, "
                f"message_id={update.message.message_id}")

    # Check if the message is forwarded
    if not update.message.forward_date:
        await update.message.reply_text("Please forward a message from a channel.")
        logger.warning(f"User {chat_id} sent a non-forwarded message")
        return

    # Check for channel message using forward_from_chat
    forwarded_channel_id = None
    if hasattr(update.message, 'forward_from_chat') and update.message.forward_from_chat and update.message.forward_from_chat.type == 'channel':
        forwarded_channel_id = str(update.message.forward_from_chat.id)
    elif str(update.message.chat.id).startswith('-100'):
        # Fallback: Assume the message is from a channel if chat.id indicates a channel
        forwarded_channel_id = str(update.message.chat.id)
        logger.info(f"Using fallback channel ID {forwarded_channel_id} for user {chat_id}")

    if not forwarded_channel_id:
        await update.message.reply_text("Please forward a message directly from a channel.")
        logger.warning(f"User {chat_id} forwarded a non-channel message: "
                      f"forward_from_chat={getattr(update.message, 'forward_from_chat', None)}")
        return

    if not forwarded_channel_id.startswith('-100'):
        await update.message.reply_text("Invalid channel ID. Please forward a message from a valid Telegram channel.")
        logger.warning(f"Invalid channel ID {forwarded_channel_id} for user {chat_id}")
        return

    logger.info(f"User {chat_id} forwarded message from channel {forwarded_channel_id}")

    try:
        # Verify bot is admin
        admins = await context.bot.get_chat_administrators(forwarded_channel_id)
        bot_id = context.bot.id
        if not any(admin.user.id == bot_id for admin in admins):
            await update.message.reply_text("I am not an admin of this channel. Please make me an admin and try again.")
            logger.warning(f"Bot is not admin of channel {forwarded_channel_id} for user {chat_id}")
            return

        # Verify user is admin
        if not any(admin.user.id == chat_id for admin in admins):
            await update.message.reply_text("Only channel admins can index movies.")
            logger.warning(f"User {chat_id} is not admin of channel {forwarded_channel_id}")
            return

        context.user_data['index_channel_id'] = forwarded_channel_id
        logger.info(f"User {chat_id} set indexing channel to {forwarded_channel_id}")

        # Initialize progress message
        progress_msg = await update.message.reply_text(
            f"Starting {context.user_data['index_mode']} indexing process...",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton('Cancel', callback_data='index_cancel')]]
            )
        )

        # Set up Telethon client
        api_id = os.getenv("TELEGRAM_API_ID")
        api_hash = os.getenv("TELEGRAM_API_HASH")
        bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

        if not all([api_id, api_hash, bot_token]):
            missing = [var for var, val in [
                ("TELEGRAM_API_ID", api_id),
                ("TELEGRAM_API_HASH", api_hash),
                ("TELEGRAM_BOT_TOKEN", bot_token)
            ] if not val]
            error_msg = f"Missing environment variables: {', '.join(missing)}"
            await update.message.reply_text(f"Configuration error: {error_msg}")
            logger.error(f"Indexing failed for channel {forwarded_channel_id}: {error_msg}")
            return

        try:
            client = TelegramClient(StringSession(), int(api_id), api_hash)
            await client.start(bot_token=bot_token)
            logger.info("TelegramClient authenticated successfully")

            total_files = 0
            duplicate = 0
            errors = 0
            unsupported = 0
            current = 0

            if context.user_data['index_mode'] == 'batch':
                # Batch indexing (assuming batch_index is defined elsewhere)
                total_files, duplicate, errors, unsupported, current = await batch_index(
                    client, forwarded_channel_id, progress_msg, context
                )
            else:
                # Single-pass indexing
                max_messages = 1000
                async for msg in client.iter_messages(int(forwarded_channel_id), limit=max_messages):
                    if not context.user_data.get('indexing'):
                        break

                    current += 1
                    try:
                        if current % 20 == 0:
                            await context.bot.edit_message_text(
                                chat_id=progress_msg.chat_id,
                                message_id=progress_msg.message_id,
                                text=(
                                    f"Single-pass indexing in progress...\n"
                                    f"Messages processed: {current}\n"
                                    f"Movies indexed: {total_files}\n"
                                    f"Duplicates skipped: {duplicate}\n"
                                    f"Unsupported skipped: {unsupported}"
                                )
                            )

                        if not msg.document or msg.document.mime_type != 'video/x-matroska':
                            unsupported += 1
                            continue

                        file_name = msg.document.attributes[-1].file_name
                        message_id = msg.id

                        language = None
                        name_lower = file_name.lower()
                        if 'tamil' in name_lower:
                            language = 'tamil'
                        elif 'english' in name_lower:
                            language = 'english'
                        elif 'hindi' in name_lower:
                            language = 'hindi'

                        try:
                            forwarded = await context.bot.forward_message(
                                chat_id=context.bot.id,
                                from_chat_id=forwarded_channel_id,
                                message_id=message_id
                            )
                            if not forwarded.document:
                                unsupported += 1
                                continue

                            file_id = forwarded.document.file_id
                            await context.bot.delete_message(
                                chat_id=context.bot.id,
                                message_id=forwarded.message_id
                            )
                        except (TelegramError, BadRequest) as te:
                            logger.error(f"Error getting file ID for {file_name}: {str(te)}")
                            errors += 1
                            continue

                        try:
                            clean_name = file_name.replace('.mkv', '').split('_')
                            title = clean_name[0].replace('.', ' ').strip()
                            year = int(clean_name[1]) if len(clean_name) > 1 and clean_name[1].isdigit() else 0
                            quality = clean_name[2] if len(clean_name) > 2 else 'Unknown'

                            size_bytes = msg.document.size
                            if size_bytes >= 1024 * 1024 * 1024:
                                file_size = f"{size_bytes / (1024 * 1024 * 1024):.2f}GB"
                            else:
                                file_size = f"{size_bytes / (1024 * 1024):.2f}MB"

                            movie_id = add_movie(
                                title=title,
                                year=year,
                                quality=quality,
                                file_size=file_size,
                                file_id=file_id,
                                message_id=message_id,
                                language=language,
                                channel_id=forwarded_channel_id
                            )

                            if movie_id:
                                total_files += 1
                                logger.info(f"Indexed: {title} ({year})")
                            else:
                                duplicate += 1
                        except (IndexError, ValueError, AttributeError) as e:
                            logger.warning(f"Error parsing {file_name}: {str(e)}")
                            errors += 1

                    except Exception as e:
                        logger.error(f"Error processing message {message_id}: {str(e)}")
                        errors += 1
                        continue

            # Final report
            result_msg = (
                f"✅ {context.user_data['index_mode'].capitalize()} indexing completed for channel {forwarded_channel_id}.\n"
                f"• Total messages processed: {current}\n"
                f"• Movies indexed: {total_files}\n"
                f"• Duplicates skipped: {duplicate}\n"
                f"• Unsupported files: {unsupported}\n"
                f"• Errors occurred: {errors}"
            )

            await context.bot.edit_message_text(
                chat_id=progress_msg.chat_id,
                message_id=progress_msg.message_id,
                text=result_msg
            )
            logger.info(f"{context.user_data['index_mode'].capitalize()} indexing completed for {forwarded_channel_id}")

        except FloodWaitError as fwe:
            await update.message.reply_text(f"Flood wait error: Please wait {fwe.seconds} seconds before trying again.")
            logger.error(f"Flood wait error: {fwe.seconds} seconds")
        except ChannelPrivateError:
            await update.message.reply_text("I don't have access to this channel. Please make sure I'm an admin.")
            logger.error("Channel access denied")
        except AuthKeyError:
            await update.message.reply_text("Authentication failed. Please check your API credentials.")
            logger.error("Telethon authentication failed")
        except RPCError as rpc_error:
            await update.message.reply_text(f"Telegram API error: {str(rpc_error)}")
            logger.error(f"RPC Error: {str(rpc_error)}")
        except Exception as e:
            await update.message.reply_text(f"Unexpected error: {str(e)}")
            logger.error(f"Indexing failed: {str(e)}", exc_info=True)
        finally:
            if 'client' in locals() and client.is_connected():
                await client.disconnect()
            context.user_data['indexing'] = False
            context.user_data['index_channel_id'] = None
            context.user_data['index_mode'] = None

    except TelegramError as te:
        await update.message.reply_text(f"Error accessing channel: {str(te)}")
        logger.error(f"Channel access error: {str(te)}")
        context.user_data['indexing'] = False
        context.user_data['index_channel_id'] = None
        context.user_data['index_mode'] = None
