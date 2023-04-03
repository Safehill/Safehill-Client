//
//  BaseRDFStore.h
//  
//
//  Created by Gennaro Frazzingaro on 7/10/21.
//

#ifndef BaseRDFStore_h
#define BaseRDFStore_h

#import <Foundation/Foundation.h>

NS_ASSUME_NONNULL_BEGIN
@protocol TripleStore <NSObject>

@property (nonatomic, retain, strong, readonly) NSString *name;

- (void)insertTripleWithSubject:(NSString *)subject predicate:(NSString *)predicate object:(NSString *)object completionHandler:(void (^)(NSError * _Nullable))completionHandler;

@end

@interface BaseRDFStore : NSObject

- (instancetype)initWithTripleStore:(id<TripleStore>)store;

- (NSArray *)executeSPARQLQuery:(NSString *)queryString
                          error:(NSError **)error __attribute__((swift_error(nonnull_error))) NS_SWIFT_NAME(execute(SPARQLQuery:));

- (void)importTriplesFromFileAtPath:(NSString *)path completionHandler:(void (^)(NSError * _Nullable))completionHandler NS_SWIFT_NAME(importTriples(fromFileAtPath:completionHandler:));

@end
NS_ASSUME_NONNULL_END

#endif /* BaseRDFStore_h */
