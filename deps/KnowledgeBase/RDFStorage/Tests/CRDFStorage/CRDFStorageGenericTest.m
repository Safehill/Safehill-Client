//
//  Test.m
//  
//
//  Created by Gennaro Frazzingaro on 7/10/21.
//

#import <XCTest/XCTest.h>
#import "BaseRDFStore.h"

@interface CRDFStorageGenericTest : XCTestCase

@end

@interface TestTripleStore: NSObject<TripleStore>
@end

@implementation TestTripleStore
@synthesize name;

- (void)insertTripleWithSubject:(nonnull NSString *)subject predicate:(nonnull NSString *)predicate object:(nonnull NSString *)object completionHandler:(nonnull void (^)(NSError * _Nullable))completionHandler {
    // TODO: Implement in-memory triple store
}

@end

@implementation CRDFStorageGenericTest

- (void)setUp {
    // Put setup code here. This method is called before the invocation of each test method in the class.
}

- (void)tearDown {
    // Put teardown code here. This method is called after the invocation of each test method in the class.
}

- (void)testExample {
    // This is an example of a functional test case.
    // Use XCTAssert and related functions to verify your tests produce the correct results.
    
    TestTripleStore *tripleStore = [[TestTripleStore alloc] init];
    BaseRDFStore *rdfStore = [[BaseRDFStore alloc] initWithTripleStore:tripleStore];
}

- (void)testPerformanceExample {
    // This is an example of a performance test case.
    [self measureBlock:^{
        // Put the code you want to measure the time of here.
    }];
}

@end
