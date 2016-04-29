////////////////////////////////////////////////////////////////////////////
//
// Copyright 2014 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#import "RLMTestCase.h"

#import "RLMRealm_Dynamic.h"
#import "RLMRealm_Private.h"

//#if !DEBUG && TARGET_OS_IPHONE && !TARGET_IPHONE_SIMULATOR

RLM_ARRAY_TYPE(CycleObject2)
@interface CycleObject1 : RLMObject
@property RLMArray<CycleObject2> *array;
@end
@implementation CycleObject1
@end

@interface CycleObject2 : RLMObject
@property CycleObject1 *obj;
@end
@implementation CycleObject2
@end

@interface PerformanceTests : RLMTestCase
@property (nonatomic) dispatch_queue_t queue;
@property (nonatomic) dispatch_semaphore_t sema;
@end

static RLMRealm *s_smallRealm, *s_mediumRealm, *s_largeRealm;

@implementation PerformanceTests

+ (void)setUp {
    [super setUp];

    s_smallRealm = [self createStringObjects:1];
    s_mediumRealm = [self createStringObjects:5];
    s_largeRealm = [self createStringObjects:50];
}

+ (void)tearDown {
    s_smallRealm = s_mediumRealm = s_largeRealm = nil;
    [RLMRealm resetRealmState];
    [super tearDown];
}

- (void)resetRealmState {
    // Do nothing, as we need to keep our in-memory realms around between tests
}

- (void)measureBlock:(void (^)(void))block {
    [super measureBlock:^{
        @autoreleasepool {
            block();
        }
    }];
}

- (void)measureMetrics:(NSArray *)metrics automaticallyStartMeasuring:(BOOL)automaticallyStartMeasuring forBlock:(void (^)(void))block {
    [super measureMetrics:metrics automaticallyStartMeasuring:automaticallyStartMeasuring forBlock:^{
        @autoreleasepool {
            block();
        }
    }];
}

+ (RLMRealm *)createStringObjects:(int)factor {
    RLMRealmConfiguration *config = [RLMRealmConfiguration new];
    config.inMemoryIdentifier = @(factor).stringValue;

    RLMRealm *realm = [RLMRealm realmWithConfiguration:config error:nil];
    [realm beginWriteTransaction];
    for (int i = 0; i < 1000 * factor; ++i) {
        [StringObject createInRealm:realm withValue:@[@"a"]];
        [StringObject createInRealm:realm withValue:@[@"b"]];
    }
    [realm commitWriteTransaction];

    return realm;
}

- (RLMRealm *)testRealm {
    RLMRealmConfiguration *config = [RLMRealmConfiguration new];
    config.inMemoryIdentifier = @"test";
    return [RLMRealm realmWithConfiguration:config error:nil];
}

- (void)testArrayStuff {
    [self measureMetrics:self.class.defaultPerformanceMetrics automaticallyStartMeasuring:NO forBlock:^{
        RLMRealm *realm = [self testRealm];
        [realm beginWriteTransaction];
        CycleObject1 *obj = [CycleObject1 createInRealm:realm withValue:@[@[]]];
        for (int i = 0; i < 500; ++i)
            [obj.array addObject:[CycleObject2 createInRealm:realm withValue:@[obj]]];
        for (int i = 0; i < 500; ++i) {
            [CycleObject1 createInRealm:realm withValue:@[[CycleObject2 allObjectsInRealm:realm]]];
        }
        [realm commitWriteTransaction];

        RLMResults *results = [CycleObject1 allObjectsInRealm:realm];
        id token = [results addNotificationBlock:^(__unused RLMResults *results, __unused RLMCollectionChange *change, __unused NSError *error) {
            CFRunLoopStop(CFRunLoopGetCurrent());
        }];
        CFRunLoopRun();

        [self startMeasuring];
        [realm beginWriteTransaction];
        [CycleObject1 createInRealm:realm withValue:@[@[]]];
        [realm commitWriteTransaction];
        CFRunLoopRun();
        [token stop];
    }];
}


- (void)observeObject:(RLMObject *)object keyPath:(NSString *)keyPath until:(int (^)(id))block {
    self.sema = dispatch_semaphore_create(0);
    self.queue = dispatch_queue_create("bg", 0);

    RLMRealmConfiguration *config = object.realm.configuration;
    NSString *className = [object.class className];
    dispatch_async(_queue, ^{
        RLMRealm *realm = [RLMRealm realmWithConfiguration:config error:nil];
        id obj = [[realm allObjects:className] firstObject];
        [obj addObserver:self forKeyPath:keyPath options:(NSKeyValueObservingOptions)0 context:(__bridge void *)_sema];

        dispatch_semaphore_signal(_sema);
        while (!block(obj)) {
            [[NSRunLoop currentRunLoop] runMode:NSDefaultRunLoopMode beforeDate:[NSDate distantFuture]];
        }

        [obj removeObserver:self forKeyPath:keyPath context:(__bridge void *)_sema];
    });
    dispatch_semaphore_wait(_sema, DISPATCH_TIME_FOREVER);
}

- (void)observeValueForKeyPath:(__unused NSString *)keyPath
                      ofObject:(__unused id)object
                        change:(__unused NSDictionary *)change
                       context:(void *)context {
    dispatch_semaphore_signal((__bridge dispatch_semaphore_t)context);
}

@end

//#endif
