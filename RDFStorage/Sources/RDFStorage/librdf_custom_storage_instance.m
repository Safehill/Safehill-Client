//
//  File.m
//  
//
//  Created by Gennaro Frazzingaro on 7/10/21.
//

#include "librdf_custom_storage_bridge.h"

#define kLocalContextUniformResourceIdentifier "file:///etc/"

/** Structure holding reference to KnowledgeBase underlying datastore */

typedef struct {
    librdf_storage *storage;
    
    void *instance;
    
    char *name;
    size_t name_len;
} librdf_storage_ck_instance;


/** Structure holding reference to a list of contexts (GRAPHs) */

typedef struct {
    librdf_storage *storage;
    librdf_storage_ck_instance* ck_context;
    
    void *ns_array;
    
    int current_index;
    int finished;
    
    librdf_node *current;
} librdf_storage_ck_get_contexts_iterator_context;


/** Manifest of storage module methods */

/* prototypes for constructors and destructors */
static int librdf_storage_ck_init(librdf_storage *storage, const char *name, librdf_hash *options);
static void librdf_storage_ck_terminate(librdf_storage *storage);

/* prototypes for opening and closing connection */
static int librdf_storage_ck_open(librdf_storage *storage, librdf_model *model);
static int librdf_storage_ck_close(librdf_storage *storage);

/* prototype for specifying features */
static librdf_node* librdf_storage_ck_get_feature(librdf_storage* storaage, librdf_uri* feature);

/* prototypes for specifying contexts */
static librdf_iterator* librdf_storage_ck_get_contexts(librdf_storage* storage);

/* prototypes for querying existance of triples */
static int librdf_storage_ck_size(librdf_storage *storage);
static int librdf_storage_ck_contains_statement(librdf_storage *storage, librdf_statement *statement);
static int librdf_storage_ck_has_arc_in(librdf_storage *storage, librdf_node *node, librdf_node *property);
static int librdf_storage_ck_has_arc_out(librdf_storage *storage, librdf_node *node, librdf_node *property);
static int librdf_storage_ck_size(librdf_storage *storage);

/* prototypes for serialization */
static librdf_stream* __nullable librdf_storage_ck_serialise(librdf_storage *storage);
static librdf_stream* __nullable librdf_storage_ck_context_serialise(librdf_storage *storage, librdf_node* context);

/* prototypes for finding statements */
static librdf_stream* __nullable librdf_storage_ck_find_statements(librdf_storage *storage, librdf_statement *statement);
static librdf_stream* __nullable librdf_storage_ck_find_statements_with_options(librdf_storage *storage, librdf_statement *statement, librdf_node* context_node, librdf_hash* options);
static librdf_stream* __nullable librdf_storage_ck_find_statements_in_context(librdf_storage* storage, librdf_statement* statement, librdf_node* context_node);

/* prototypes for finding nodes and arcs */
static librdf_iterator* __nullable librdf_storage_ck_find_sources(librdf_storage *storage, librdf_node *arc, librdf_node *target);
static librdf_iterator* __nullable librdf_storage_ck_find_arcs(librdf_storage *storage, librdf_node *src, librdf_node *target);
static librdf_iterator* __nullable librdf_storage_ck_find_targets(librdf_storage *storage, librdf_node *src, librdf_node *arc);
static librdf_iterator* __nullable librdf_storage_ck_get_arcs_in(librdf_storage *storage, librdf_node *node);
static librdf_iterator* __nullable librdf_storage_ck_get_arcs_out(librdf_storage *storage, librdf_node *node);

/** Entry point for dynamically loaded storage module */
static void librdf_storage_ck_register_factory(librdf_storage_factory *factory);


void
librdf_storage_module_register_factory(librdf_world *world)
{
    librdf_storage_register_factory(world,
                                    kDefaultCustomStorageIdentifier,
                                    "SQLite",
                                    &librdf_storage_ck_register_factory);
}

/** Local entry point for dynamically loaded storage module */
static void
librdf_storage_ck_register_factory(librdf_storage_factory *factory)
{
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused"
    int name_matches = !strcmp(factory->name, kDefaultCustomStorageIdentifier);
    NSString *errorMessage = [NSString stringWithFormat:@"librdf_storage_ck_register_factory only handles factories with name %s.", kDefaultCustomStorageIdentifier];
#pragma clang diagnostic pop
    NSCAssert(name_matches, errorMessage);
    
    factory->version            = 1;
    factory->init               = librdf_storage_ck_init;
    factory->terminate          = librdf_storage_ck_terminate;
    factory->open               = librdf_storage_ck_open;
    factory->close              = librdf_storage_ck_close;
    factory->get_feature        = librdf_storage_ck_get_feature;
    factory->get_contexts       = librdf_storage_ck_get_contexts;
    factory->size               = librdf_storage_ck_size;
    factory->contains_statement = librdf_storage_ck_contains_statement;
    factory->has_arc_in         = librdf_storage_ck_has_arc_in;
    factory->has_arc_in         = librdf_storage_ck_has_arc_out;
    factory->serialise          = librdf_storage_ck_serialise;
    factory->context_serialise  = librdf_storage_ck_context_serialise;
    factory->find_statements    = librdf_storage_ck_find_statements;
    factory->find_statements_with_options = librdf_storage_ck_find_statements_with_options;
    factory->find_statements_in_context = librdf_storage_ck_find_statements_in_context;
    factory->find_sources       = librdf_storage_ck_find_sources;
    factory->find_arcs          = librdf_storage_ck_find_arcs;
    factory->find_targets       = librdf_storage_ck_find_targets;
    factory->get_arcs_in        = librdf_storage_ck_get_arcs_in;
    factory->get_arcs_out       = librdf_storage_ck_get_arcs_out;
}


/* functions implementing storage api */
static int
librdf_storage_ck_init(librdf_storage *storage, const char *name, librdf_hash *options) {
    
    char *name_copy;
    librdf_storage_ck_instance *context;
    
    if (NULL == name) {
//        CKLogErrorFramework("rdf storage requires a name");
        return 1;
    }
    
    context = (librdf_storage_ck_instance *)calloc(1, sizeof(librdf_storage_ck_instance));
    
    if (NULL == context) {
        return 1;
    }
    
    librdf_storage_set_instance(storage, context);
    
    context->storage = storage;
//    CKKnowledgeStore *knowledgeStore = [CKKnowledgeStore knowledgeStoreWithName:[NSString stringWithUTF8String:name]];
//    context->instance = (__bridge_retained void*)(knowledgeStore);
//    context->name_len = strlen(name);
//    name_copy = (char *)calloc(context->name_len + 1, sizeof(char*));
//
//    if (NULL == name_copy) {
//        return 1;
//    }
//
//    strncpy(name_copy, name, context->name_len);
//    context->name = name_copy;
    
    return 0;
}


static void
librdf_storage_ck_terminate(librdf_storage *storage)
{
    librdf_storage_ck_instance *context;
    
    context = (librdf_storage_ck_instance*)librdf_storage_get_instance(storage);
    
    if (context == NULL) {
        return;
    }
    
    if (context->name) {
        free(context->name);
    }
}

static int
librdf_storage_ck_open(librdf_storage *storage, librdf_model *model)
{
    // Nothing to do, the lifecycle of a connection is managed by the underlying storage
    return 0;
}

static int
librdf_storage_ck_close(librdf_storage *storage)
{
    // Nothing to do, the lifecycle of a connection is managed by the underlying storage
    return 0;
}

static librdf_node* librdf_storage_ck_get_feature(librdf_storage* storage, librdf_uri* feature)
{
    /* librdf_storage_sqlite_instance* scontext; */
    unsigned char *uri_string;
    
    librdf_world *world;
    world = librdf_storage_get_world(storage);
    
    if(!feature) {
        return NULL;
    }
    
    uri_string = librdf_uri_as_string(feature);
    if(!uri_string) {
        return NULL;
    }
    
    if(!strcmp((const char*)uri_string, LIBRDF_MODEL_FEATURE_CONTEXTS)) {
        return librdf_new_node_from_typed_literal(world,
                                                  (const unsigned char*)"1",
                                                  NULL, NULL);
    }
    
    return NULL;
}

static void
librdf_context_iterator_finished(void * __nullable iterator)
{
    if (NULL == iterator) {
        return;
    }
    
    librdf_storage_ck_get_contexts_iterator_context* itr;
    itr = (librdf_storage_ck_get_contexts_iterator_context*)iterator;
    
    if(NULL != itr->storage) {
        librdf_storage_remove_reference(itr->storage);
    }
    if (NULL != itr->ns_array) {
        CFRelease(itr->ns_array);
    }
    
    free(itr);
}

static int
librdf_context_iterator_is_end(void* iterator)
{
    librdf_storage_ck_get_contexts_iterator_context* itr;
    itr = (librdf_storage_ck_get_contexts_iterator_context*)iterator;
    
    if (NULL == itr || NULL == itr->storage) {
        return 1;
    }
    
    return itr->finished;
}

static int
librdf_context_iterator_next(void* iterator)
{
    librdf_storage_ck_get_contexts_iterator_context* context;
    NSArray *contexts;
    
    context = (librdf_storage_ck_get_contexts_iterator_context*)iterator;
    if (NULL == context || NULL == context->storage) {
        return 1;
    }
    contexts = (__bridge NSArray *)context->ns_array;
    context->current_index++;
    
    if (context->current_index >= [contexts count]) {
        context->finished = 1;
        return 1;
    }
    
    return 0;
}

static void*
librdf_context_iterator_get(void* iterator, int flags)
{
    librdf_world *world;
    librdf_storage_ck_get_contexts_iterator_context* itr;
    NSArray<NSString *> *contexts;
    NSString *context;
    
    itr = (librdf_storage_ck_get_contexts_iterator_context*)iterator;
    
    if (NULL == itr || NULL == itr->storage) {
        return NULL;
    }
    
    world = librdf_storage_get_world(itr->storage);
    
    if (NULL == world) {
        return NULL;
    }
    
    contexts = (__bridge NSArray *)(itr->ns_array);
    if ([contexts count] == 0) {
        return NULL;
    }
    context = contexts[itr->current_index];
    
    librdf_uri *uri;
    uri = librdf_new_uri(world, (const unsigned char *)[context UTF8String]);
    return librdf_new_node_from_uri(world, uri);
}

static librdf_iterator* librdf_storage_ck_get_contexts(librdf_storage* storage)
{
    librdf_iterator *iterator;
    librdf_storage_ck_get_contexts_iterator_context* itr;
    
    itr = (librdf_storage_ck_get_contexts_iterator_context *)calloc(1, sizeof(librdf_storage_ck_get_contexts_iterator_context));
    
    if(NULL == itr) {
        return NULL;
    }
    
    itr->storage = storage;
    librdf_storage_add_reference(itr->storage);
    
    itr->ns_array = (__bridge_retained void *)(@[@kLocalContextUniformResourceIdentifier]);
    itr->current_index = 0;
    itr->finished = 0;
    
    iterator = librdf_new_iterator(librdf_storage_get_world(storage),
                                   itr,
                                   &librdf_context_iterator_is_end,
                                   &librdf_context_iterator_next,
                                   &librdf_context_iterator_get,
                                   &librdf_context_iterator_finished);
    
    if(!iterator) {
        librdf_context_iterator_finished((void*)iterator);
        return NULL;
    }
    
    return iterator;
}

static int
librdf_storage_ck_size(librdf_storage *storage)
{
    librdf_storage_ck_instance *context;
    context = (librdf_storage_ck_instance*)librdf_storage_get_instance(storage);
    
    if (NULL == context) {
        return -1;
    }
    
    return 0;
    
//    CKKnowledgeStore *store = (__bridge CKKnowledgeStore *)context->instance;
//
//    if (NULL == store) {
//        return 0;
//    }
//
//    NSError *error;
//    NSArray *triples = triplesMatchingCondition(store, nil, &error);
//    if (error != nil) {
//        CKLogErrorFramework("%@", [error localizedDescription]);
//        return -1;
//    }
//
//    return (int)[triples count];
}

static int
librdf_storage_ck_contains_triple(librdf_storage *storage, librdf_node *src, librdf_node *arc, librdf_node *target) {
    
    librdf_storage_ck_instance *context;
    context = (librdf_storage_ck_instance*)librdf_storage_get_instance(storage);
    
    if (NULL == context) {
//        CKLogErrorFramework("undefined context object");
        return 0;
    }
    
    return 0;
    
//    CKKnowledgeStore *store = (__bridge CKKnowledgeStore *)context->instance;
//
//    if (NULL == store) {
//        CKLogErrorFramework("undefined store in context object");
//        return 0;
//    }
//
//    NSString *s, *p, *o;
//    if (src) {
//        s = librdf_node_get_nsstring_value(src);
//    }
//    if (arc) {
//        p = librdf_node_get_nsstring_value(arc);
//    }
//    if (target) {
//        o = librdf_node_get_nsstring_value(target);
//    }
//
//    CKTripleCondition *condition = [[CKTripleCondition alloc] initWithSubject:s predicate:p object:o];
//
//    NSError *error = nil;
//    NSArray *triples = triplesMatchingCondition(store, condition, &error);
//    if (error != nil) {
//        CKLogErrorFramework("%@", [error localizedDescription]);
//        return 0;
//    }
//
//    return (int)[triples count];
}

static int
librdf_storage_ck_contains_statement(librdf_storage *storage, librdf_statement *statement)
{
    return librdf_storage_ck_contains_triple(storage, statement->subject, statement->predicate, statement->object);
}

static int
librdf_storage_ck_has_arc_in(librdf_storage *storage, librdf_node *node, librdf_node *property)
{
    return librdf_storage_ck_contains_triple(storage, node, property, 0);
}

static int
librdf_storage_ck_has_arc_out(librdf_storage *storage, librdf_node *node, librdf_node *property)
{
    return librdf_storage_ck_contains_triple(storage, 0, property, node);
}

static librdf_stream* __nullable
librdf_storage_ck_serialise(librdf_storage *storage)
{
    return librdf_storage_ck_find_statements(storage, NULL);
}

static librdf_stream* __nullable
librdf_storage_ck_context_serialise(librdf_storage *storage, librdf_node* context)
{
    NSString *context_uri_string = librdf_node_get_nsstring_value(context);
    
    if (nil == context_uri_string) {
        return librdf_storage_ck_find_statements(storage, NULL);
    }
    
    if ([context_uri_string isEqualToString:@kLocalContextUniformResourceIdentifier]) {
        // Handle Local Context query
    }
    
    return NULL;
    
}


/**
 * librdf_storage_ck_find_statements:
 * @storage: the storage
 * @statement: the statement to match
 *
 * .
 *
 * Return a stream of statements matching the given statement (or
 * all statements if NULL).  Parts (subject, predicate, object) of the
 * statement can be empty in which case any statement part will match that.
 * Uses #librdf_statement_match to do the matching.
 *
 * Return value: a #librdf_stream or NULL on failure
 **/
static librdf_stream* __nullable
librdf_storage_ck_find_statements(librdf_storage *storage, librdf_statement *statement)
{
    librdf_storage_ck_instance *context;
    NSString *s, *p, *o, *graph;
    NSError *error;
    
    return NULL;
//    CKKnowledgeStore *store;
//
//    context = (librdf_storage_ck_instance*)librdf_storage_get_instance(storage);
//
//    if (NULL == context) {
//        CKLogErrorFramework("undefined context object");
//        return NULL;
//    }
//
//    store = (__bridge CKKnowledgeStore *)context->instance;
//
//    if (NULL == store) {
//        CKLogErrorFramework("undefined store in context object");
//        return NULL;
//    }
//
//    if (NULL != statement) {
//        // ->graph seems to be always NULL?
//        if (NULL != statement->graph) {
//            graph = librdf_node_get_nsstring_value(statement->graph);
//        }
//        if (NULL != statement->subject) {
//            s = librdf_node_get_nsstring_value(statement->subject);
//        }
//        if (NULL != statement->predicate) {
//            p = librdf_node_get_nsstring_value(statement->predicate);
//        }
//        if (NULL != statement->object) {
//            o = librdf_node_get_nsstring_value(statement->object);
//        }
//    }
//
//    CKTripleCondition *condition = [[CKTripleCondition alloc] initWithSubject:s predicate:p object:o];
//    NSArray *triples = triplesMatchingCondition(store, condition, &error);
//    if (error != nil) {
//        CKLogErrorFramework("%@", error);
//        return NULL;
//    }
//
//    return librdf_new_ck_rdf_stream(storage, triples);
}

static librdf_stream* __nullable
librdf_storage_ck_find_statements_in_context(librdf_storage* storage, librdf_statement* statement, librdf_node* context_node)
{
    NSString *context_uri_string = librdf_node_get_nsstring_value(context_node);
    
    if (nil == context_uri_string) {
        return librdf_storage_ck_find_statements(storage, statement);
    }
    
    if ([context_uri_string isEqualToString:@kLocalContextUniformResourceIdentifier]) {
        // Handle Local Context query
    }
    
    return NULL;
}

static librdf_stream* __nullable librdf_storage_ck_find_statements_with_options(librdf_storage *storage, librdf_statement *statement, librdf_node* context_node, librdf_hash* options)
{
    return librdf_storage_ck_find_statements(storage, statement);
}

static librdf_iterator* __nullable
librdf_storage_ck_get_triples(librdf_storage *storage, librdf_node *src, librdf_node *arc, librdf_node *target, librdf_ck_desired_field desiredResult)
{
    NSArray *triples = @[];
    
    librdf_storage_ck_instance *context;
    context = (librdf_storage_ck_instance*)librdf_storage_get_instance(storage);
    
    return NULL;
//    if (NULL == context) {
//        CKLogErrorFramework("undefined context object");
//        return NULL;
//    }
//
//    if (NULL == context->instance) {
//        CKLogErrorFramework("undefined instance in context object");
//        return NULL;
//    }
//
//    NSString *s, *p, *o;
//    if (NULL != src) {
//        s = librdf_node_get_nsstring_value(src);
//    }
//    if (NULL != arc) {
//        p = librdf_node_get_nsstring_value(arc);
//    }
//    if (NULL != target) {
//        o = librdf_node_get_nsstring_value(target);
//    }
//
//    CKKnowledgeStore *store = (__bridge CKKnowledgeStore *)context->instance;
//
//    if (NULL == store) {
//        CKLogErrorFramework("undefined CKKnowledgeStore in instance object");
//        return NULL;
//    }
//
//    CKTripleCondition *condition = [[CKTripleCondition alloc] initWithSubject:s predicate:p object:o];
//
//    NSError *error = nil;
//    triples = triplesMatchingCondition(store, condition, &error);
//
//    if (error != nil) {
//        CKLogErrorFramework("%@", [error localizedDescription]);
//        return NULL;
//    }
//
//    librdf_iterator *result;
//    result = librdf_new_triple_iterator(storage, triples, desiredResult);
//    return result;
}

static librdf_iterator* __nullable
librdf_storage_ck_find_sources(librdf_storage *storage, librdf_node *arc, librdf_node *target)
{
    return librdf_storage_ck_get_triples(storage, 0, arc, target, RDF_CK_FIELD_SOURCE);
}

static librdf_iterator* __nullable
librdf_storage_ck_find_arcs(librdf_storage *storage, librdf_node *src, librdf_node *target)
{
    return librdf_storage_ck_get_triples(storage, src, 0, target, RDF_CK_FIELD_ARC);
}

static librdf_iterator* __nullable
librdf_storage_ck_find_targets(librdf_storage *storage, librdf_node *src, librdf_node *arc)
{
    return librdf_storage_ck_get_triples(storage, src, arc, 0, RDF_CK_FIELD_TARGET);
}

static librdf_iterator* __nullable
librdf_storage_ck_get_arcs_in(librdf_storage *storage, librdf_node *node)
{
    return librdf_storage_ck_get_triples(storage, 0, 0, node, RDF_CK_FIELD_ARC);
}

static librdf_iterator* __nullable
librdf_storage_ck_get_arcs_out(librdf_storage *storage, librdf_node *node)
{
    return librdf_storage_ck_get_triples(storage, node, 0, 0, RDF_CK_FIELD_ARC);
}

