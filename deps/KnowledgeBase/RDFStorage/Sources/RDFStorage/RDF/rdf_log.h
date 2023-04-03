/* -*- Mode: c; c-basic-offset: 2 -*-
 *
 * rdf_log.h - RDF logging interfaces
 *
 * Copyright (C) 2004-2008, David Beckett http://www.dajobe.org/
 * Copyright (C) 2004-2005, University of Bristol, UK http://www.bristol.ac.uk/
 * 
 * This package is Free Software and part of Redland http://librdf.org/
 * 
 * It is licensed under the following three licenses as alternatives:
 *   1. GNU Lesser General Public License (LGPL) V2.1 or any newer version
 *   2. GNU General Public License (GPL) V2 or any newer version
 *   3. Apache License, V2.0 or any newer version
 * 
 * You may not use this file except in compliance with at least one of
 * the above three licenses.
 * 
 * See LICENSE.html or LICENSE.txt at the top of this package for the
 * complete terms and further detail along with the license texts for
 * the licenses in COPYING.LIB, COPYING and LICENSE-2.0.txt respectively.
 * 
 * 
 */


#ifndef LIBRDF_LOG_H
#define LIBRDF_LOG_H

#ifdef __cplusplus
extern "C" {
#endif

#include "../Raptor/raptor2.h"

/**
 * librdf_log_level:
 * @LIBRDF_LOG_NONE: No level
 * @LIBRDF_LOG_DEBUG: Debug.
 * @LIBRDF_LOG_INFO: Information.
 * @LIBRDF_LOG_WARN: Warning.
 * @LIBRDF_LOG_ERROR: Recoverable error.  Program can continue.
 * @LIBRDF_LOG_FATAL: Fatal error.  Program will abort if this is not caught.
 * @LIBRDF_LOG_LAST: Internal, never returned.
 *
 * Indicates the level of the log message.
 */
typedef enum {
  LIBRDF_LOG_NONE = 0,
  LIBRDF_LOG_DEBUG,
  LIBRDF_LOG_INFO,
  LIBRDF_LOG_WARN,
  LIBRDF_LOG_ERROR,
  LIBRDF_LOG_FATAL,
  LIBRDF_LOG_LAST=LIBRDF_LOG_FATAL
} librdf_log_level;
  

/**
 * librdf_log_facility:
 * @LIBRDF_FROM_CONCEPTS: Concepts
 * @LIBRDF_FROM_DIGEST: Digest
 * @LIBRDF_FROM_FILES: Files
 * @LIBRDF_FROM_HASH: Hash
 * @LIBRDF_FROM_INIT: Init
 * @LIBRDF_FROM_ITERATOR: Iterator
 * @LIBRDF_FROM_LIST: List
 * @LIBRDF_FROM_MODEL: Model
 * @LIBRDF_FROM_NODE: Node
 * @LIBRDF_FROM_PARSER: Parser
 * @LIBRDF_FROM_QUERY: Query
 * @LIBRDF_FROM_SERIALIZER: Serializer
 * @LIBRDF_FROM_STATEMENT: Statement
 * @LIBRDF_FROM_STORAGE: Storage
 * @LIBRDF_FROM_STREAM: Stream
 * @LIBRDF_FROM_URI: URI
 * @LIBRDF_FROM_UTF8: UTF8
 * @LIBRDF_FROM_MEMORY: Memory
 * @LIBRDF_FROM_NONE: Associated with no part.
 * @LIBRDF_FROM_RAPTOR: Raptor library (parser or serializer; Raptor 2.0.0+).
 * @LIBRDF_FROM_LAST: Internal, never returned.
 *
 * Indicates the part of the system that generated the log message.
 */
typedef enum {
  LIBRDF_FROM_NONE = 0,
  LIBRDF_FROM_CONCEPTS,
  LIBRDF_FROM_DIGEST,
  LIBRDF_FROM_FILES,
  LIBRDF_FROM_HASH,
  LIBRDF_FROM_INIT,
  LIBRDF_FROM_ITERATOR,
  LIBRDF_FROM_LIST,
  LIBRDF_FROM_MODEL,
  LIBRDF_FROM_NODE,
  LIBRDF_FROM_PARSER,
  LIBRDF_FROM_QUERY,
  LIBRDF_FROM_SERIALIZER,
  LIBRDF_FROM_STATEMENT,
  LIBRDF_FROM_STORAGE,
  LIBRDF_FROM_STREAM,
  LIBRDF_FROM_URI,
  LIBRDF_FROM_UTF8,
  LIBRDF_FROM_MEMORY,
  LIBRDF_FROM_RAPTOR,
  LIBRDF_FROM_LAST=LIBRDF_FROM_RAPTOR
} librdf_log_facility;
  

/**
 * librdf_log_message:
 *
 * Structure for storing parts of a log message generated by Redland.
 */
typedef struct 
{
  int code;                  /* The error code */
  librdf_log_level level;
  librdf_log_facility facility;
  const char *message;
  /* valid for certain facilities such as LIBRDF_FROM_PARSER */
  raptor_locator *locator;
} librdf_log_message;


/**
 * librdf_log_level_func:
 * @user_data: User data pointer
 * @message: Log message.
 * @arguments: Message arguments.
 *
 * Handler for one log level, for the warning and error levels ONLY.
 * Used by #librdf_world_set_warning and #librdf_world_set_error.
 *
 * Return value: non-zero to indicate log message has been handled 
 */
typedef int (REDLAND_CALLBACK_STDCALL *librdf_log_level_func)(void *user_data, const char *message, va_list arguments);

/**
 * librdf_log_func:
 * @user_data: User data pointer
 * @message: Log message structure pointer.
 *
 * Handler for all log levels.
 *
 * Return value: non-zero to indicate log message has been handled 
 */
typedef int (REDLAND_CALLBACK_STDCALL *librdf_log_func)(void *user_data, librdf_log_message *message);

#ifdef LIBRDF_INTERNAL
#ifdef IS_MODULE
#include "rdf_log_internal.h"
#else
#include <rdf_log_internal.h>
#endif
#endif


/* log message accessors */
REDLAND_API
int librdf_log_message_code(librdf_log_message *message);
REDLAND_API
librdf_log_level librdf_log_message_level(librdf_log_message *message);
REDLAND_API
librdf_log_facility librdf_log_message_facility(librdf_log_message *message);
REDLAND_API
const char * librdf_log_message_message(librdf_log_message *message);
REDLAND_API
raptor_locator* librdf_log_message_locator(librdf_log_message *message);

/* logging functions */
REDLAND_API
void librdf_log_simple(librdf_world* world, int code, librdf_log_level level, librdf_log_facility facility, void *locator, const char *message);
REDLAND_API
void librdf_log(librdf_world* world, int code, librdf_log_level level, librdf_log_facility facility, void *locator, const char *message, ...) REDLAND_PRINTF_FORMAT(6, 7);

#ifdef __cplusplus
}
#endif

#endif
