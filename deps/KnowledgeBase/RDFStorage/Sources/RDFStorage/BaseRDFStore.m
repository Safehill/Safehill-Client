//
//  BaseRDFStore.m
//  
//
//  Created by Gennaro Frazzingaro on 7/10/21.
//

#import "BaseRDFStore.h"
#import "librdf_custom_storage_bridge.h"

#ifndef kSparqlQueryLanguageIdentifier
#define kSparqlQueryLanguageIdentifier "sparql"
#endif

const char * const kDefaultCustomStorageIdentifier = "com.gf.rdf.storage";
const int kGenericQueryErrorCode = -1;

@implementation BaseRDFStore {
    id<TripleStore> _tripleStore;
}

- (instancetype)initWithTripleStore:(id<TripleStore>)store
{
    self = [super init];
    if (self) {
        _tripleStore = store;
    }
    return self;
}

- (NSArray *)executeSPARQLQuery:(NSString *)queryString error:(NSError **)error {
    
    NSMutableArray *results = [NSMutableArray new];
    
    librdf_world *world = librdf_new_world();
    assert(NULL != world);
    
    librdf_world_open(world);
    
    librdf_uri *base_uri = librdf_new_uri(world, (const unsigned char *)".");
    if (NULL == base_uri) {
        librdf_free_world(world);
        
        if (error) {
            NSString *bundleId = [NSString stringWithCString:kDefaultCustomStorageIdentifier encoding:NSUTF8StringEncoding];
            *error = [NSError errorWithDomain:bundleId
                                         code:kGenericQueryErrorCode
                                     userInfo:@{
                                                @"localizedDescription": @"Can not initialize librdf_world",
                                                @"query": queryString,
                                                @"language": @kSparqlQueryLanguageIdentifier}];
        }
        
        return results;
    }
    
    librdf_storage_module_register_factory(world);
    librdf_query *query = librdf_new_query(world,
                                           kSparqlQueryLanguageIdentifier,
                                           0,
                                           (unsigned char *)[queryString UTF8String],
                                           base_uri);
    
    if (NULL == query) {
        librdf_free_uri(base_uri);
        librdf_free_world(world);
        
        if (error) {
            NSString *bundleId = [NSString stringWithCString:kDefaultCustomStorageIdentifier encoding:NSUTF8StringEncoding];
            *error = [NSError errorWithDomain:bundleId
                                         code:kGenericQueryErrorCode
                                     userInfo:@{
                                                @"localizedDescription": @"Can not initialize librdf_uri",
                                                @"query": queryString,
                                                @"language": @kSparqlQueryLanguageIdentifier
                                                }];
        }
        
        return results;
    }
    
    librdf_storage *storage = librdf_new_storage(world,
                                                 kDefaultCustomStorageIdentifier,
                                                 [[_tripleStore name] UTF8String],
                                                 "contexts='yes'");
    
    if (NULL == storage) {
        librdf_free_query(query);
        librdf_free_uri(base_uri);
        librdf_free_world(world);
        
        if (error) {
            NSString *bundleId = [NSString stringWithCString:kDefaultCustomStorageIdentifier encoding:NSUTF8StringEncoding];
            *error = [NSError errorWithDomain:bundleId
                                         code:kGenericQueryErrorCode
                                     userInfo:@{
                                                @"localizedDescription": @"Can not initialize librdf_storage",
                                                @"query": queryString,
                                                @"language": @kSparqlQueryLanguageIdentifier
                                                }];
        }
        
        return results;
    }
    
    librdf_model *model = librdf_new_model(world, storage, "contexts='yes'");
    
    if (NULL == model) {
        librdf_free_storage(storage);
        librdf_free_query(query);
        librdf_free_uri(base_uri);
        librdf_free_world(world);
        
        if (error) {
            NSString *bundleId = [NSString stringWithCString:kDefaultCustomStorageIdentifier encoding:NSUTF8StringEncoding];
            *error = [NSError errorWithDomain:bundleId
                                         code:kGenericQueryErrorCode
                                     userInfo:@{
                                                
                                                @"localizedDescription": @"Can not initialize librdf_model",
                                                @"query": queryString,
                                                @"language": @kSparqlQueryLanguageIdentifier
                                                }];
        }
        
        return results;
    }
    
    librdf_query_results *rdf_results = librdf_query_execute(query, model);
    NSDictionary *resultsDictionary = librdf_query_results_get_nsdictionary_all_bindings(rdf_results);
    while (resultsDictionary) {
        [results addObject:resultsDictionary];
        resultsDictionary = librdf_query_results_get_nsdictionary_all_bindings(rdf_results);
    }
    
    librdf_free_query_results(rdf_results);
    librdf_free_storage(storage);
    librdf_free_model(model);
    librdf_free_query(query);
    librdf_free_uri(base_uri);
    librdf_free_world(world);
    
    return results;
}

void handle_statement(void *store, raptor_statement *statement) {
    NSCharacterSet *toTrim  = [NSCharacterSet characterSetWithCharactersInString:@"\"<>"];
    
    NSString *subj = [@((char *)raptor_term_to_string(statement->subject)) stringByTrimmingCharactersInSet:toTrim];
    NSString *pred = [@((char *)raptor_term_to_string(statement->predicate)) stringByTrimmingCharactersInSet:toTrim];
    NSString *obj = [@((char *)raptor_term_to_string(statement->object)) stringByTrimmingCharactersInSet:toTrim];
    
    [(__bridge id<TripleStore>)store insertTripleWithSubject:subj predicate:pred object:obj completionHandler:^(NSError * error) {
//        CKLogErrorFramework("SPARQL insert error");
    }];
}

- (void)importTriplesFromFileAtPath:(NSString *)path completionHandler:(void (^)(NSError * _Nullable))completionHandler {
    
    dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_BACKGROUND, 0), ^{
        raptor_world *rworld = NULL;
        raptor_parser* rdf_parser = NULL;
        unsigned char *uri_string;
        raptor_uri *ruri, *raptor_base_uri;
        
        rworld = raptor_new_world();
        
        rdf_parser = raptor_new_parser(rworld, "turtle");
        
        raptor_parser_set_statement_handler(rdf_parser, (__bridge void *)(_tripleStore), handle_statement);
        
        uri_string = raptor_uri_filename_to_uri_string([path UTF8String]);
        ruri = raptor_new_uri(rworld, uri_string);
        raptor_base_uri = raptor_uri_copy(ruri);
        
        raptor_parser_parse_file(rdf_parser, ruri, raptor_base_uri);
        
        raptor_free_parser(rdf_parser);
        
        raptor_free_uri(raptor_base_uri);
        raptor_free_uri(ruri);
        raptor_free_memory(uri_string);
        raptor_free_world(rworld);
        
        completionHandler(nil);
    });
}

@end
