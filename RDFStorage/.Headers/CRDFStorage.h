#include "../Raptor/raptor.h"
#include "../RDF/redland.h"

#import <Foundation/Foundation.h>

NS_ASSUME_NONNULL_BEGIN

#pragma once

typedef struct librdf_storage_ck_results_iterator_s librdf_storage_ck_results_iterator;
typedef struct librdf_storage_ck_stream_context_s librdf_storage_ck_stream_context;

typedef enum {
    RDF_CK_FIELD_SOURCE,
    RDF_CK_FIELD_ARC,
    RDF_CK_FIELD_TARGET
} librdf_ck_desired_field;


void librdf_storage_module_register_factory(librdf_world *world);

NSDictionary *librdf_query_results_get_nsdictionary_all_bindings(librdf_query_results *result);

NSString *librdf_node_get_nsstring_value(librdf_node *node);
librdf_iterator* __nullable librdf_new_triple_iterator(librdf_storage *storage, NSArray *triples, librdf_ck_desired_field desired);


NS_ASSUME_NONNULL_END

