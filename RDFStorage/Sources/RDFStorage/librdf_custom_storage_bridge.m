//
//  librdf_custom_storage_bridge.m
//
//
//  Created by Gennaro Frazzingaro on 7/9/21.
//


#import "librdf_custom_storage_bridge.h"

NSDictionary *librdf_query_results_get_nsdictionary_current_bindings(librdf_query_results *result);

NSDictionary *librdf_query_results_get_nsdictionary_all_bindings(librdf_query_results *result)
{
    if (NULL == result || librdf_query_results_finished(result)) {
//        CKLogErrorFramework("no result");
        return nil;
    }
    
    NSDictionary *bindings = librdf_query_results_get_nsdictionary_current_bindings(result);
    librdf_query_results_next(result);
    return bindings;
}

NSDictionary *librdf_query_results_get_nsdictionary_current_bindings(librdf_query_results *result)
{
    const char **names = NULL;
    librdf_node **values;
    NSMutableDictionary *bindings = [NSMutableDictionary new];
    NSInteger bindingsCount = librdf_query_results_get_bindings_count(result);
    NSInteger i = 0;
    
//    CKLogDebugFramework("found %ld bindings", (long)bindingsCount);
    
    values = (librdf_node **)calloc(bindingsCount, sizeof(librdf_node *));
    if (NULL == values) {
        return bindings;
    }
    librdf_query_results_get_bindings(result, &names, values);
    for (; i<bindingsCount; i++) {
        id object = [NSNull null];
        if (NULL != values[i]) {
            librdf_node *node = values[i];
            object = librdf_node_get_nsstring_value(node);
            if (object == nil) {
                object = [NSNull null];
            }
            librdf_free_node(node);
        }
        [bindings setObject:object forKey:[NSString stringWithUTF8String:names[i]]];
    }
    free(values);
    
//    CKLogDebugFramework("bindings = %@", bindings);
    
    return bindings;
}

NSString *librdf_node_get_nsstring_value(librdf_node *node)
{
    if (NULL == node) {
        return nil;
    }
    
    librdf_uri *uri_value;
    char *cString = NULL;
    
    switch (node->type) {
        case RAPTOR_TERM_TYPE_URI:
            uri_value = librdf_node_get_uri(node);
            cString = (char *)raptor_uri_as_string(uri_value);
            break;
        case RAPTOR_TERM_TYPE_LITERAL:
            cString = (char *)librdf_node_get_literal_value(node);
            break;
        case RAPTOR_TERM_TYPE_BLANK:
            cString = (char *)librdf_node_get_blank_identifier(node);
            break;
//        case RAPTOR_TERM_TYPE_UNKNOWN:
        default:
//            CKLogErrorDaemon("node type not supported. type=%d", node->type);
            break;
    }
    
    if (NULL == cString) {
        return nil;
    }
    
    return [[NSString alloc] initWithUTF8String:cString];
}

librdf_statement *librdf_statement_from_ck_triple(librdf_world *world, NSArray *triple)
{
    assert([triple count] == 3);
    const char *subjectS = [[triple objectAtIndex:0] UTF8String];
    const char *predicateS = [[triple objectAtIndex:1] UTF8String];
    const char *objectS = [[triple objectAtIndex:2] UTF8String];
    
    librdf_statement *statement;
    librdf_node *subject, *predicate, *object;
    
    subject = librdf_new_node_from_literal(world, (const unsigned char *)subjectS, NULL,0);
    object = librdf_new_node_from_literal(world, (const unsigned char *)objectS, NULL,0);
    predicate = librdf_new_node_from_literal(world, (const unsigned char *)predicateS, NULL,0);

    statement = librdf_new_statement(world);
    librdf_statement_set_subject(statement, subject);
    librdf_statement_set_object(statement, object);
    librdf_statement_set_predicate(statement, predicate);
    return statement;
}

//NSArray * __nullable triplesMatchingCondition(CKKnowledgeStore * __nonnull store, CKTripleCondition * __nullable condition, NSError * __nullable __autoreleasing *error) {
//    __block NSArray *triples;
//    dispatch_semaphore_t semaphore = dispatch_semaphore_create(0);
//    
//    [store triplesMatching:condition completionHandler:^(NSError *queryError, NSArray<CKTriple *> *result) {
//        *error = queryError;
//        triples = result;
//        dispatch_semaphore_signal(semaphore);
//    }];
//    
//    dispatch_semaphore_wait(semaphore, DISPATCH_TIME_FOREVER);
//    
//    return triples;
//}
