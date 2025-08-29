package circuitbreaker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/anilonayy/circuit-breaker/options"
	"github.com/stretchr/testify/assert"
)

func TestNewCircuitBreaker(t *testing.T) {
	t.Logf("🔧 TEST START: Creating new circuit breaker with key 'test-service'")
	startTime := time.Now()

	cb := NewCircuitBreaker("test-service")
	t.Logf("✅ Circuit breaker created successfully in %v", time.Since(startTime))

	// Verify key assignment
	expectedKey := "test-service"
	actualKey := cb.GetKey()
	if actualKey != expectedKey {
		t.Errorf("❌ Key mismatch - Expected: '%s', Got: '%s'", expectedKey, actualKey)
	} else {
		t.Logf("✅ Circuit breaker key verified: '%s'", actualKey)
	}

	// Verify initial state
	expectedState := StateClosed
	actualState := cb.GetState()
	if actualState != expectedState {
		t.Errorf("❌ Initial state mismatch - Expected: %s, Got: %s", expectedState, actualState)
	} else {
		t.Logf("✅ Initial circuit breaker state verified: %s", actualState)
	}

	t.Logf("🏁 TEST COMPLETE: NewCircuitBreaker test passed in %v", time.Since(startTime))
}

func TestCircuitBreakerStates(t *testing.T) {
	t.Logf("🔧 TEST START: Circuit Breaker State Transitions")
	testStartTime := time.Now()

	// Configuration details
	timeWindow := 10 * time.Second
	minRequests := int64(3)
	failureThreshold := 0.5
	openTimeout := 1 * time.Second

	t.Logf("📋 Configuration: TimeWindow=%v, MinRequests=%d, FailureThreshold=%.1f%%, OpenTimeout=%v",
		timeWindow, minRequests, failureThreshold*100, openTimeout)

	cb := NewCircuitBreaker("test-service",
		options.WithTimeWindow(timeWindow),
		options.WithMinimumRequestThreshold(minRequests),
		options.WithFailureRateThreshold(failureThreshold),
		options.WithOpenTimeout(openTimeout),
	)
	t.Logf("✅ Circuit breaker created with key: %s", cb.GetKey())

	ctx := context.Background()

	// Test successful requests - should stay closed
	t.Logf("🧪 PHASE 1: Testing successful requests (circuit should remain CLOSED)")
	successPhaseStart := time.Now()
	successCount := 0

	for i := range 5 {
		requestStart := time.Now()
		t.Logf("📤 Executing successful request %d/5", i+1)

		err := cb.Execute(ctx, func(ctx context.Context) error {
			return nil
		})

		requestDuration := time.Since(requestStart)
		if err != nil {
			t.Errorf("❌ Request %d failed unexpectedly: %v (duration: %v)", i+1, err, requestDuration)
		} else {
			successCount++
			t.Logf("✅ Request %d succeeded (duration: %v)", i+1, requestDuration)
		}
	}

	successPhaseDuration := time.Since(successPhaseStart)
	currentState := cb.GetState()
	totalReqs, successes, failures, failRate := cb.GetStatistics()

	if currentState != StateClosed {
		t.Errorf("❌ Expected state to remain CLOSED, got %s", currentState)
	} else {
		t.Logf("✅ Circuit state verified: %s", currentState)
	}

	t.Logf("📊 Success Phase Summary: %d/%d requests succeeded in %v", successCount, 5, successPhaseDuration)
	t.Logf("📊 Circuit Statistics: Total=%d, Success=%d, Failure=%d, FailureRate=%.2f%%",
		totalReqs, successes, failures, failRate*100)

	// Test failing requests - should open circuit
	t.Logf("🧪 PHASE 2: Testing failing requests (circuit should open after threshold)")
	failurePhaseStart := time.Now()
	failureCount := 0

	for i := range 5 {
		requestStart := time.Now()
		t.Logf("📤 Executing failing request %d/5", i+1)

		err := cb.Execute(ctx, func(ctx context.Context) error {
			return errors.New("test error")
		})

		requestDuration := time.Since(requestStart)
		if err == nil {
			t.Errorf("❌ Request %d unexpectedly succeeded (duration: %v)", i+1, requestDuration)
		} else {
			failureCount++
			t.Logf("✅ Request %d failed as expected: %v (duration: %v)", i+1, err, requestDuration)
		}

		// Check state after each failure
		currentState := cb.GetState()
		totalReqs, successes, failures, failRate := cb.GetStatistics()
		t.Logf("📊 After request %d: State=%s, Stats(T:%d,S:%d,F:%d,R:%.1f%%)",
			i+1, currentState, totalReqs, successes, failures, failRate*100)
	}

	failurePhaseDuration := time.Since(failurePhaseStart)
	finalState := cb.GetState()
	totalReqs, successes, failures, failRate = cb.GetStatistics()

	// Should be open now
	if finalState != StateOpen {
		t.Errorf("❌ Expected state to be OPEN, got %s", finalState)
	} else {
		t.Logf("✅ Circuit state verified: %s", finalState)
	}

	t.Logf("📊 Failure Phase Summary: %d/%d requests failed in %v", failureCount, 5, failurePhaseDuration)
	t.Logf("📊 Final Circuit Statistics: Total=%d, Success=%d, Failure=%d, FailureRate=%.2f%%",
		totalReqs, successes, failures, failRate*100)

	// Test that requests are blocked when open
	t.Logf("🧪 PHASE 3: Testing request blocking when circuit is OPEN")
	blockingTestStart := time.Now()

	t.Logf("📤 Attempting request on OPEN circuit")
	err := cb.Execute(ctx, func(ctx context.Context) error {
		return nil
	})

	blockingTestDuration := time.Since(blockingTestStart)

	if err != ErrCircuitBreakerOpen {
		t.Errorf("❌ Expected ErrCircuitBreakerOpen, got %v (duration: %v)", err, blockingTestDuration)
	} else {
		t.Logf("✅ Request correctly blocked: %v (duration: %v)", err, blockingTestDuration)
	}

	totalTestDuration := time.Since(testStartTime)
	t.Logf("🏁 TEST COMPLETE: Circuit Breaker State Transitions completed in %v", totalTestDuration)
	t.Logf("📈 Performance Summary: Success Phase: %v, Failure Phase: %v, Blocking Test: %v",
		successPhaseDuration, failurePhaseDuration, blockingTestDuration)
}

func TestCircuitBreakerTimeout(t *testing.T) {
	t.Logf("🔧 TEST START: Circuit Breaker Timeout Handling")
	testStartTime := time.Now()

	requestTimeout := 100 * time.Millisecond
	simulatedDelay := 200 * time.Millisecond
	highThreshold := int64(100) // Prevent circuit opening during timeout test

	t.Logf("📋 Configuration: RequestTimeout=%v, SimulatedDelay=%v, MinThreshold=%d",
		requestTimeout, simulatedDelay, highThreshold)

	cb := NewCircuitBreaker("test-service-timeout",
		options.WithRequestTimeout(requestTimeout),
		options.WithMinimumRequestThreshold(highThreshold),
	)
	t.Logf("✅ Circuit breaker created with key: %s", cb.GetKey())

	ctx := context.Background()

	t.Logf("🧪 Testing timeout behavior with slow operation")
	t.Logf("📤 Executing operation that should timeout (delay: %v > timeout: %v)", simulatedDelay, requestTimeout)

	operationStart := time.Now()
	err := cb.Execute(ctx, func(ctx context.Context) error {
		t.Logf("🔄 Operation started, simulating %v delay", simulatedDelay)
		// Simulate slow operation that should timeout
		select {
		case <-time.After(simulatedDelay):
			t.Logf("⏰ Simulated operation completed naturally (should not reach here)")
			return nil
		case <-ctx.Done():
			operationDuration := time.Since(operationStart)
			t.Logf("🛑 Operation cancelled after %v due to: %v", operationDuration, ctx.Err())
			return ctx.Err()
		}
	})

	totalDuration := time.Since(operationStart)

	if err != ErrCircuitBreakerTimeout {
		t.Errorf("❌ Expected ErrCircuitBreakerTimeout, got %v (duration: %v)", err, totalDuration)
	} else {
		t.Logf("✅ Timeout error correctly returned: %v (duration: %v)", err, totalDuration)
	}

	// Verify timeout duration is reasonable
	expectedMin := requestTimeout - 10*time.Millisecond // Allow some tolerance
	expectedMax := requestTimeout + 50*time.Millisecond // Allow some overhead

	if totalDuration < expectedMin {
		t.Errorf("❌ Timeout happened too quickly: %v < expected minimum %v", totalDuration, expectedMin)
	} else if totalDuration > expectedMax {
		t.Errorf("⚠️  Timeout took longer than expected: %v > expected maximum %v", totalDuration, expectedMax)
	} else {
		t.Logf("✅ Timeout duration within expected range: %v (expected: %v ± tolerance)", totalDuration, requestTimeout)
	}

	// Check final circuit statistics
	totalReqs, successes, failures, failRate := cb.GetStatistics()
	state := cb.GetState()

	t.Logf("📊 Final Statistics: State=%s, Total=%d, Success=%d, Failure=%d, FailureRate=%.2f%%",
		state, totalReqs, successes, failures, failRate*100)

	testDuration := time.Since(testStartTime)
	t.Logf("🏁 TEST COMPLETE: Timeout handling test completed in %v", testDuration)
}

func TestCircuitBreakerStatistics(t *testing.T) {
	t.Logf("🔧 TEST START: Circuit Breaker Statistics Tracking")
	testStartTime := time.Now()

	highThreshold := int64(100) // Prevent circuit opening during stats test
	t.Logf("📋 Configuration: MinThreshold=%d (high to prevent opening)", highThreshold)

	cb := NewCircuitBreaker("test-service-stats",
		options.WithMinimumRequestThreshold(highThreshold),
	)
	t.Logf("✅ Circuit breaker created with key: %s", cb.GetKey())

	ctx := context.Background()

	// Execute some requests - pattern: fail on 0, 3, 6, 9 (4 failures), succeed on others (6 successes)
	t.Logf("🧪 Testing statistics tracking with predictable failure pattern")
	t.Logf("📊 Pattern: fail on indices 0,3,6,9 (4 failures), succeed on others (6 successes)")

	expectedSuccesses := 0
	expectedFailures := 0
	totalRequests := 10

	// Pre-calculate expected results
	for i := range totalRequests {
		if i%3 == 0 {
			expectedFailures++
		} else {
			expectedSuccesses++
		}
	}

	t.Logf("📈 Expected Results: Total=%d, Success=%d, Failure=%d, Rate=%.1f%%",
		totalRequests, expectedSuccesses, expectedFailures, float64(expectedFailures)/float64(totalRequests)*100)

	executionStart := time.Now()
	actualSuccesses := 0
	actualFailures := 0

	for i := range totalRequests {
		index := i // Capture loop variable
		requestStart := time.Now()

		willFail := index%3 == 0
		t.Logf("📤 Executing request %d/%d (index %d) - Expected: %s",
			i+1, totalRequests, index, map[bool]string{true: "FAIL", false: "SUCCESS"}[willFail])

		err := cb.Execute(ctx, func(ctx context.Context) error {
			if willFail {
				return errors.New("test error")
			}
			return nil
		})

		requestDuration := time.Since(requestStart)

		if err != nil {
			actualFailures++
			if !willFail {
				t.Errorf("❌ Request %d unexpectedly failed: %v (duration: %v)", i+1, err, requestDuration)
			} else {
				t.Logf("✅ Request %d failed as expected: %v (duration: %v)", i+1, err, requestDuration)
			}
		} else {
			actualSuccesses++
			if willFail {
				t.Errorf("❌ Request %d unexpectedly succeeded (duration: %v)", i+1, requestDuration)
			} else {
				t.Logf("✅ Request %d succeeded as expected (duration: %v)", i+1, requestDuration)
			}
		}

		// Show running statistics every few requests
		if (i+1)%3 == 0 || i == totalRequests-1 {
			currentTotal, currentSuccess, currentFailure, currentRate := cb.GetStatistics()
			t.Logf("📊 Running Stats after request %d: T=%d, S=%d, F=%d, R=%.1f%%",
				i+1, currentTotal, currentSuccess, currentFailure, currentRate*100)
		}
	}

	executionDuration := time.Since(executionStart)

	// Get final statistics
	finalTotal, finalSuccess, finalFailure, finalRate := cb.GetStatistics()
	t.Logf("📊 Final Statistics: Total=%d, Success=%d, Failure=%d, Rate=%.2f%% (collected in %v)",
		finalTotal, finalSuccess, finalFailure, finalRate*100, executionDuration)

	// Verify total requests
	if finalTotal != int64(totalRequests) {
		t.Errorf("❌ Total requests mismatch - Expected: %d, Got: %d", totalRequests, finalTotal)
	} else {
		t.Logf("✅ Total requests verified: %d", finalTotal)
	}

	// Verify success count
	if finalSuccess != int64(expectedSuccesses) {
		t.Errorf("❌ Success count mismatch - Expected: %d, Got: %d", expectedSuccesses, finalSuccess)
	} else {
		t.Logf("✅ Success count verified: %d", finalSuccess)
	}

	// Verify failure count
	if finalFailure != int64(expectedFailures) {
		t.Errorf("❌ Failure count mismatch - Expected: %d, Got: %d", expectedFailures, finalFailure)
	} else {
		t.Logf("✅ Failure count verified: %d", finalFailure)
	}

	// Verify failure rate
	expectedFailureRate := float64(expectedFailures) / float64(totalRequests)
	tolerance := 0.01
	if finalRate < expectedFailureRate-tolerance || finalRate > expectedFailureRate+tolerance {
		t.Errorf("❌ Failure rate mismatch - Expected: %.2f±%.2f, Got: %.2f",
			expectedFailureRate, tolerance, finalRate)
	} else {
		t.Logf("✅ Failure rate verified: %.2f%% (expected: %.2f%%)", finalRate*100, expectedFailureRate*100)
	}

	// Verify execution vs expected counts
	t.Logf("📈 Execution Summary: Actual Success=%d/%d, Actual Failure=%d/%d",
		actualSuccesses, expectedSuccesses, actualFailures, expectedFailures)

	testDuration := time.Since(testStartTime)
	t.Logf("🏁 TEST COMPLETE: Statistics tracking test completed in %v", testDuration)
}

func TestCustomErrorPredicate(t *testing.T) {
	t.Logf("🔧 TEST START: Custom Error Predicate Logic")
	testStartTime := time.Now()

	customError := errors.New("custom error")
	otherError := errors.New("other error")
	highThreshold := int64(100) // Prevent circuit opening during test

	t.Logf("📋 Configuration: CustomError='%v', OtherError='%v', MinThreshold=%d",
		customError, otherError, highThreshold)

	t.Logf("🔧 Creating circuit breaker with custom error predicate")
	cb := NewCircuitBreaker("test-service-custom-error",
		options.WithCustomErrorPredicate(func(err error) bool {
			isCustomError := err == customError
			t.Logf("🔍 Error predicate called: error='%v', isCustomError=%t", err, isCustomError)
			return isCustomError
		}),
		options.WithMinimumRequestThreshold(highThreshold),
	)
	t.Logf("✅ Circuit breaker created with key: %s", cb.GetKey())

	ctx := context.Background()

	// Test 1: Non-custom error should not be considered a failure
	t.Logf("🧪 PHASE 1: Testing non-custom error (should be treated as success)")
	t.Logf("📤 Executing request with non-custom error: '%v'", otherError)

	phase1Start := time.Now()
	err := cb.Execute(ctx, func(ctx context.Context) error {
		return otherError
	})
	phase1Duration := time.Since(phase1Start)

	if err == nil {
		t.Errorf("❌ Expected error to be returned, got nil (duration: %v)", phase1Duration)
	} else if err.Error() != otherError.Error() {
		t.Errorf("❌ Expected error '%v', got '%v' (duration: %v)", otherError, err, phase1Duration)
	} else {
		t.Logf("✅ Non-custom error correctly returned: %v (duration: %v)", err, phase1Duration)
	}

	// Check statistics - should show success because error predicate returned false
	total1, success1, failure1, rate1 := cb.GetStatistics()
	t.Logf("📊 Statistics after non-custom error: Total=%d, Success=%d, Failure=%d, Rate=%.1f%%",
		total1, success1, failure1, rate1*100)

	if success1 != 1 || failure1 != 0 {
		t.Errorf("❌ Statistics mismatch - Expected: Success=1, Failure=0, Got: Success=%d, Failure=%d",
			success1, failure1)
	} else {
		t.Logf("✅ Statistics verified: Non-custom error treated as success")
	}

	// Test 2: Custom error should be considered a failure
	t.Logf("🧪 PHASE 2: Testing custom error (should be treated as failure)")
	t.Logf("📤 Executing request with custom error: '%v'", customError)

	phase2Start := time.Now()
	err = cb.Execute(ctx, func(ctx context.Context) error {
		return customError
	})
	phase2Duration := time.Since(phase2Start)

	if err == nil {
		t.Errorf("❌ Expected error to be returned, got nil (duration: %v)", phase2Duration)
	} else if err.Error() != customError.Error() {
		t.Errorf("❌ Expected error '%v', got '%v' (duration: %v)", customError, err, phase2Duration)
	} else {
		t.Logf("✅ Custom error correctly returned: %v (duration: %v)", err, phase2Duration)
	}

	// Check statistics - should show failure
	total2, success2, failure2, rate2 := cb.GetStatistics()
	t.Logf("📊 Statistics after custom error: Total=%d, Success=%d, Failure=%d, Rate=%.1f%%",
		total2, success2, failure2, rate2*100)

	if success2 != 1 || failure2 != 1 {
		t.Errorf("❌ Statistics mismatch - Expected: Success=1, Failure=1, Got: Success=%d, Failure=%d",
			success2, failure2)
	} else {
		t.Logf("✅ Statistics verified: Custom error treated as failure")
	}

	// Summary
	expectedFinalRate := 0.5 // 1 failure out of 2 total requests
	if rate2 < expectedFinalRate-0.01 || rate2 > expectedFinalRate+0.01 {
		t.Errorf("❌ Final failure rate mismatch - Expected: %.1f%%, Got: %.1f%%",
			expectedFinalRate*100, rate2*100)
	} else {
		t.Logf("✅ Final failure rate verified: %.1f%%", rate2*100)
	}

	testDuration := time.Since(testStartTime)
	t.Logf("📈 Performance Summary: Phase1=%v, Phase2=%v, Total=%v",
		phase1Duration, phase2Duration, testDuration)
	t.Logf("🏁 TEST COMPLETE: Custom error predicate test completed in %v", testDuration)
}

func TestCircuitBreakerRegistry(t *testing.T) {
	t.Logf("🔧 TEST START: Circuit Breaker Registry Management")
	testStartTime := time.Now()

	// Clean up registry first
	t.Logf("🧹 PHASE 1: Cleaning up existing circuit breakers")
	initialKeys := ListCircuitBreakers()
	t.Logf("📊 Found %d existing circuit breakers: %v", len(initialKeys), initialKeys)

	cleanupStart := time.Now()
	for _, key := range initialKeys {
		t.Logf("🗑️  Removing existing circuit breaker: %s", key)
		RemoveCircuitBreaker(key)
	}
	cleanupDuration := time.Since(cleanupStart)

	// Verify cleanup
	afterCleanupKeys := ListCircuitBreakers()
	if len(afterCleanupKeys) != 0 {
		t.Errorf("❌ Cleanup failed - Expected 0 circuit breakers, got %d: %v", len(afterCleanupKeys), afterCleanupKeys)
	} else {
		t.Logf("✅ Registry cleaned up successfully in %v", cleanupDuration)
	}

	// Create circuit breakers
	t.Logf("🧪 PHASE 2: Creating new circuit breakers")
	createStart := time.Now()

	t.Logf("🔧 Creating circuit breaker: service-1")
	cb1 := NewCircuitBreaker("service-1")
	t.Logf("✅ Created service-1 with key: %s", cb1.GetKey())

	t.Logf("🔧 Creating circuit breaker: service-2")
	cb2 := NewCircuitBreaker("service-2")
	t.Logf("✅ Created service-2 with key: %s", cb2.GetKey())

	createDuration := time.Since(createStart)

	// Test listing
	t.Logf("🧪 PHASE 3: Testing registry listing")
	listStart := time.Now()
	keys := ListCircuitBreakers()
	listDuration := time.Since(listStart)

	t.Logf("📊 Listed circuit breakers (%d): %v (duration: %v)", len(keys), keys, listDuration)
	if len(keys) != 2 {
		t.Errorf("❌ Expected 2 circuit breakers, got %d", len(keys))
	} else {
		t.Logf("✅ Registry listing verified: %d circuit breakers", len(keys))
	}

	// Test getting existing circuit breaker
	t.Logf("🧪 PHASE 4: Testing circuit breaker retrieval")
	retrievalStart := time.Now()

	t.Logf("🔍 Retrieving existing circuit breaker: service-1")
	if retrievedCB, exists := GetCircuitBreaker("service-1"); !exists {
		t.Errorf("❌ Expected to find service-1 circuit breaker")
	} else if retrievedCB != cb1 {
		t.Errorf("❌ Retrieved circuit breaker is not the same instance (expected: %p, got: %p)", cb1, retrievedCB)
	} else {
		t.Logf("✅ Successfully retrieved circuit breaker: %s (same instance: %p)", retrievedCB.GetKey(), retrievedCB)
	}

	// Test getting non-existent circuit breaker
	t.Logf("🔍 Testing retrieval of non-existent circuit breaker")
	if retrievedCB, exists := GetCircuitBreaker("non-existent"); exists {
		t.Errorf("❌ Expected not to find non-existent circuit breaker, but got: %v", retrievedCB)
	} else {
		t.Logf("✅ Correctly did not find non-existent circuit breaker")
	}

	retrievalDuration := time.Since(retrievalStart)

	// Test creating duplicate - should return existing
	t.Logf("🧪 PHASE 5: Testing duplicate creation handling")
	duplicateStart := time.Now()

	t.Logf("🔧 Creating duplicate circuit breaker: service-1")
	cb1Duplicate := NewCircuitBreaker("service-1")
	duplicateDuration := time.Since(duplicateStart)

	if cb1Duplicate != cb1 {
		t.Errorf("❌ Expected to get existing circuit breaker instance (original: %p, duplicate: %p)", cb1, cb1Duplicate)
	} else {
		t.Logf("✅ Successfully returned existing circuit breaker instance: %p (duration: %v)", cb1Duplicate, duplicateDuration)
	}

	// Test removal
	t.Logf("🧪 PHASE 6: Testing circuit breaker removal")
	removalStart := time.Now()

	t.Logf("🗑️  Removing circuit breaker: service-1")
	RemoveCircuitBreaker("service-1")

	if retrievedCB, exists := GetCircuitBreaker("service-1"); exists {
		t.Errorf("❌ Expected service-1 to be removed, but still found: %v", retrievedCB)
	} else {
		t.Logf("✅ Successfully removed circuit breaker: service-1")
	}

	// Verify remaining count
	remainingKeys := ListCircuitBreakers()
	if len(remainingKeys) != 1 {
		t.Errorf("❌ Expected 1 remaining circuit breaker, got %d: %v", len(remainingKeys), remainingKeys)
	} else {
		t.Logf("✅ Registry count verified: %d remaining circuit breakers: %v", len(remainingKeys), remainingKeys)
	}

	removalDuration := time.Since(removalStart)

	// Clean up remaining
	t.Logf("🧹 PHASE 7: Final cleanup")
	finalCleanupStart := time.Now()

	t.Logf("🗑️  Cleaning up remaining circuit breaker: service-2")
	RemoveCircuitBreaker("service-2")

	finalKeys := ListCircuitBreakers()
	if len(finalKeys) != 0 {
		t.Errorf("❌ Expected empty registry after cleanup, got %d: %v", len(finalKeys), finalKeys)
	} else {
		t.Logf("✅ Final cleanup successful - registry is empty")
	}

	finalCleanupDuration := time.Since(finalCleanupStart)

	// Performance summary
	testDuration := time.Since(testStartTime)
	t.Logf("📈 Performance Summary:")
	t.Logf("📈   Cleanup: %v", cleanupDuration)
	t.Logf("📈   Creation: %v", createDuration)
	t.Logf("📈   Listing: %v", listDuration)
	t.Logf("📈   Retrieval: %v", retrievalDuration)
	t.Logf("📈   Duplicate: %v", duplicateDuration)
	t.Logf("📈   Removal: %v", removalDuration)
	t.Logf("📈   Final Cleanup: %v", finalCleanupDuration)
	t.Logf("🏁 TEST COMPLETE: Registry management test completed in %v", testDuration)
}

func TestStateStrings(t *testing.T) {
	t.Logf("🔧 TEST START: Circuit Breaker State String Representations")
	testStartTime := time.Now()

	tests := []struct {
		state       State
		expected    string
		description string
	}{
		{StateClosed, "CLOSED", "Normal operation state"},
		{StateOpen, "OPEN", "Circuit breaker opened due to failures"},
		{StateHalfOpen, "HALF_OPEN", "Testing state after timeout"},
		{State(999), "UNKNOWN", "Invalid/undefined state"},
	}

	t.Logf("📋 Testing %d state string representations", len(tests))

	successCount := 0
	failureCount := 0

	for i, test := range tests {
		testStart := time.Now()
		t.Logf("🧪 Test case %d/%d: State(%d) - %s", i+1, len(tests), test.state, test.description)
		t.Logf("📤 Expected string: '%s'", test.expected)

		actualString := test.state.String()
		testDuration := time.Since(testStart)

		if actualString != test.expected {
			failureCount++
			t.Errorf("❌ State string mismatch for State(%d)", test.state)
			t.Errorf("   Expected: '%s'", test.expected)
			t.Errorf("   Got:      '%s'", actualString)
			t.Errorf("   Duration: %v", testDuration)
		} else {
			successCount++
			t.Logf("✅ State(%d) correctly returns: '%s' (duration: %v)", test.state, actualString, testDuration)
		}
	}

	t.Logf("📊 Test Results Summary:")
	t.Logf("📊   Total Tests: %d", len(tests))
	t.Logf("📊   Passed: %d", successCount)
	t.Logf("📊   Failed: %d", failureCount)
	t.Logf("📊   Success Rate: %.1f%%", float64(successCount)/float64(len(tests))*100)

	if failureCount > 0 {
		t.Errorf("❌ %d out of %d state string tests failed", failureCount, len(tests))
	} else {
		t.Logf("✅ All state string representations are correct")
	}

	testDuration := time.Since(testStartTime)
	t.Logf("🏁 TEST COMPLETE: State string representation test completed in %v", testDuration)
}

func TestWithOptions(t *testing.T) {
	t.Logf("🔧 TEST START: Circuit Breaker Custom Options Configuration")
	testStartTime := time.Now()

	// Custom configuration
	timeWindow := 2 * time.Minute
	minRequests := int64(100) // High threshold to prevent opening
	failureThreshold := 0.7
	openTimeout := 45 * time.Second
	requestTimeout := 15 * time.Second
	maxHalfOpenRequests := int64(5)

	t.Logf("📋 Custom Configuration:")
	t.Logf("📋   TimeWindow: %v", timeWindow)
	t.Logf("📋   MinRequests: %d (high to prevent opening)", minRequests)
	t.Logf("📋   FailureThreshold: %.1f%%", failureThreshold*100)
	t.Logf("📋   OpenTimeout: %v", openTimeout)
	t.Logf("📋   RequestTimeout: %v", requestTimeout)
	t.Logf("📋   MaxHalfOpenRequests: %d", maxHalfOpenRequests)

	configStart := time.Now()
	cb := NewCircuitBreaker("test-service-options",
		options.WithTimeWindow(timeWindow),
		options.WithMinimumRequestThreshold(minRequests),
		options.WithFailureRateThreshold(failureThreshold),
		options.WithOpenTimeout(openTimeout),
		options.WithRequestTimeout(requestTimeout),
		options.WithThresholdRequestInHalfOpen(maxHalfOpenRequests),
	)
	configDuration := time.Since(configStart)
	t.Logf("✅ Circuit breaker created with custom options in %v", configDuration)

	// Test that options were applied (we can't directly access private fields,
	// but we can test behavior that depends on them)

	t.Logf("🧪 PHASE 1: Verifying key assignment")
	expectedKey := "test-service-options"
	actualKey := cb.GetKey()
	if actualKey != expectedKey {
		t.Errorf("❌ Key mismatch - Expected: '%s', Got: '%s'", expectedKey, actualKey)
	} else {
		t.Logf("✅ Circuit breaker key verified: '%s'", actualKey)
	}

	// Test timeout option by trying a slow operation
	t.Logf("🧪 PHASE 2: Testing custom request timeout configuration")
	ctx := context.Background()

	simulatedDelay := 20 * time.Second // Longer than configured timeout
	t.Logf("📋 Timeout Test Setup:")
	t.Logf("📋   Configured Timeout: %v", requestTimeout)
	t.Logf("📋   Simulated Delay: %v", simulatedDelay)
	t.Logf("📋   Expected: Timeout after ~%v", requestTimeout)

	t.Logf("📤 Executing slow operation to test timeout")
	operationStart := time.Now()
	err := cb.Execute(ctx, func(ctx context.Context) error {
		t.Logf("🔄 Starting slow operation (will be cancelled by timeout)")
		// Simulate slow operation that should timeout
		select {
		case <-time.After(simulatedDelay):
			t.Logf("⏰ Operation completed naturally (should not reach here)")
			return nil
		case <-ctx.Done():
			actualDuration := time.Since(operationStart)
			t.Logf("🛑 Operation cancelled after %v due to: %v", actualDuration, ctx.Err())
			return ctx.Err()
		}
	})
	totalDuration := time.Since(operationStart)

	// Verify timeout error
	if err != ErrCircuitBreakerTimeout {
		t.Errorf("❌ Expected timeout error, got %v (duration: %v)", err, totalDuration)
	} else {
		t.Logf("✅ Timeout error correctly returned: %v (duration: %v)", err, totalDuration)
	}

	// Verify timeout duration is reasonable
	expectedMin := requestTimeout - 1*time.Second // Allow some tolerance
	expectedMax := requestTimeout + 2*time.Second // Allow some overhead

	t.Logf("📊 Timeout Duration Analysis:")
	t.Logf("📊   Expected Range: %v - %v", expectedMin, expectedMax)
	t.Logf("📊   Actual Duration: %v", totalDuration)

	if totalDuration < expectedMin {
		t.Errorf("❌ Timeout happened too quickly: %v < expected minimum %v", totalDuration, expectedMin)
	} else if totalDuration > expectedMax {
		t.Errorf("⚠️  Timeout took longer than expected: %v > expected maximum %v", totalDuration, expectedMax)
	} else {
		t.Logf("✅ Timeout duration within expected range")
	}

	// Test circuit state and statistics
	t.Logf("🧪 PHASE 3: Verifying circuit state after timeout")
	state := cb.GetState()
	totalReqs, successes, failures, failRate := cb.GetStatistics()

	t.Logf("📊 Circuit Status after timeout test:")
	t.Logf("📊   State: %s", state)
	t.Logf("📊   Total Requests: %d", totalReqs)
	t.Logf("📊   Successes: %d", successes)
	t.Logf("📊   Failures: %d", failures)
	t.Logf("📊   Failure Rate: %.1f%%", failRate*100)

	if state != StateClosed {
		t.Logf("⚠️  Circuit state is %s (expected CLOSED with high threshold)", state)
	} else {
		t.Logf("✅ Circuit remains CLOSED as expected (high threshold prevents opening)")
	}

	testDuration := time.Since(testStartTime)
	t.Logf("📈 Performance Summary:")
	t.Logf("📈   Configuration: %v", configDuration)
	t.Logf("📈   Timeout Test: %v", totalDuration)
	t.Logf("📈   Total Test: %v", testDuration)

	t.Logf("🏁 TEST COMPLETE: Custom options configuration test completed in %v", testDuration)
}

func TestGetDetailedStatisticsReturnsStruct(t *testing.T) {
	t.Logf("🔧 TEST START: Detailed Statistics Structure Verification")
	testStartTime := time.Now()

	testKey := "test-key"
	t.Logf("📋 Configuration: Testing with key '%s' and default options", testKey)

	setupStart := time.Now()
	cb := NewCircuitBreaker(testKey)
	setupDuration := time.Since(setupStart)
	t.Logf("✅ Circuit breaker created in %v", setupDuration)

	// Execute a few requests to generate some statistics
	t.Logf("🧪 PHASE 1: Generating test data")
	dataGenStart := time.Now()

	t.Logf("📤 Executing successful request")
	successStart := time.Now()
	err1 := cb.Execute(context.Background(), func(ctx context.Context) error {
		return nil // Success
	})
	successDuration := time.Since(successStart)

	if err1 != nil {
		t.Errorf("❌ Unexpected error in success request: %v (duration: %v)", err1, successDuration)
	} else {
		t.Logf("✅ Success request completed (duration: %v)", successDuration)
	}

	t.Logf("📤 Executing failing request")
	failureStart := time.Now()
	err2 := cb.Execute(context.Background(), func(ctx context.Context) error {
		return errors.New("test error") // Failure
	})
	failureDuration := time.Since(failureStart)

	if err2 == nil {
		t.Errorf("❌ Expected error in failure request, got nil (duration: %v)", failureDuration)
	} else {
		t.Logf("✅ Failure request completed as expected: %v (duration: %v)", err2, failureDuration)
	}

	dataGenDuration := time.Since(dataGenStart)
	t.Logf("📊 Test data generation completed in %v", dataGenDuration)

	t.Logf("🧪 PHASE 2: Retrieving detailed statistics")
	statsStart := time.Now()
	stats := cb.GetDetailedStatistics()
	statsDuration := time.Since(statsStart)
	t.Logf("✅ Detailed statistics retrieved in %v", statsDuration)

	// Verify the struct fields are populated correctly
	t.Logf("🧪 PHASE 3: Verifying detailed statistics fields")
	verificationStart := time.Now()

	// Core identification
	t.Logf("🔍 Verifying core identification fields")
	assert.Equal(t, testKey, stats.Key)
	t.Logf("✅ Key verified: %s", stats.Key)

	assert.Equal(t, "CLOSED", stats.State)
	t.Logf("✅ State verified: %s", stats.State)

	// Request statistics
	t.Logf("🔍 Verifying request statistics")
	assert.Equal(t, int64(2), stats.TotalRequests)
	t.Logf("✅ Total requests verified: %d", stats.TotalRequests)

	assert.Equal(t, int64(1), stats.SuccessCount)
	t.Logf("✅ Success count verified: %d", stats.SuccessCount)

	assert.Equal(t, int64(1), stats.FailureCount)
	t.Logf("✅ Failure count verified: %d", stats.FailureCount)

	assert.Equal(t, 0.5, stats.FailureRate)
	t.Logf("✅ Failure rate verified: %.1f%%", stats.FailureRate*100)

	// Configuration parameters
	t.Logf("🔍 Verifying configuration parameters")
	assert.Equal(t, int64(60), stats.TimeWindowSeconds) // Default 1 minute
	t.Logf("✅ Time window verified: %d seconds", stats.TimeWindowSeconds)

	assert.Equal(t, int64(10), stats.MinimumRequestThreshold)
	t.Logf("✅ Minimum request threshold verified: %d", stats.MinimumRequestThreshold)

	assert.Equal(t, 0.5, stats.FailureRateThreshold)
	t.Logf("✅ Failure rate threshold verified: %.1f%%", stats.FailureRateThreshold*100)

	assert.Equal(t, int64(30), stats.OpenTimeoutSeconds) // Default 30 seconds
	t.Logf("✅ Open timeout verified: %d seconds", stats.OpenTimeoutSeconds)

	assert.Equal(t, int64(5), stats.RequestTimeoutSeconds) // Default 5 seconds
	t.Logf("✅ Request timeout verified: %d seconds", stats.RequestTimeoutSeconds)

	assert.Equal(t, int64(10), stats.ThresholdRequestInHalfOpen) // Default threshold
	t.Logf("✅ Threshold requests in half-open verified: %d", stats.ThresholdRequestInHalfOpen)

	// Verify optional fields are nil when not applicable
	t.Logf("🔍 Verifying optional fields (should be nil in CLOSED state)")
	assert.Nil(t, stats.OpenedAt)
	t.Logf("✅ OpenedAt is nil (circuit not opened)")

	assert.Nil(t, stats.NextHalfOpenAt)
	t.Logf("✅ NextHalfOpenAt is nil (circuit not opened)")

	assert.Nil(t, stats.TimeUntilHalfOpenSeconds)
	t.Logf("✅ TimeUntilHalfOpenSeconds is nil (circuit not opened)")

	assert.Nil(t, stats.HalfOpenRequestCount)
	t.Logf("✅ HalfOpenRequestCount is nil (circuit not in half-open)")

	assert.Nil(t, stats.RemainingTestRequests)
	t.Logf("✅ RemainingTestRequests is nil (circuit not in half-open)")

	verificationDuration := time.Since(verificationStart)

	// Summary
	t.Logf("📊 Detailed Statistics Summary:")
	t.Logf("📊   Key: %s", stats.Key)
	t.Logf("📊   State: %s", stats.State)
	t.Logf("📊   Requests: Total=%d, Success=%d, Failure=%d", stats.TotalRequests, stats.SuccessCount, stats.FailureCount)
	t.Logf("📊   Failure Rate: %.1f%%", stats.FailureRate*100)
	t.Logf("📊   Configuration: TimeWindow=%ds, MinRequests=%d, FailureThreshold=%.1f%%",
		stats.TimeWindowSeconds, stats.MinimumRequestThreshold, stats.FailureRateThreshold*100)
	t.Logf("📊   Timeouts: Open=%ds, Request=%ds", stats.OpenTimeoutSeconds, stats.RequestTimeoutSeconds)
	t.Logf("📊   Threshold Requests in Half-Open: %d", stats.ThresholdRequestInHalfOpen)

	testDuration := time.Since(testStartTime)
	t.Logf("📈 Performance Summary:")
	t.Logf("📈   Setup: %v", setupDuration)
	t.Logf("📈   Data Generation: %v", dataGenDuration)
	t.Logf("📈   Statistics Retrieval: %v", statsDuration)
	t.Logf("📈   Verification: %v", verificationDuration)
	t.Logf("📈   Total Test: %v", testDuration)

	t.Logf("🏁 TEST COMPLETE: Detailed statistics verification completed successfully in %v", testDuration)
}

func TestCircuitBreakerOpenToHalfOpenTransition(t *testing.T) {
	t.Logf("🔧 TEST START: OPEN → HALF_OPEN → CLOSED State Transition")
	testStartTime := time.Now()

	// Create circuit breaker with short open timeout for faster testing
	openTimeout := 2 * time.Second
	thresholdRequestsInHalfOpen := int64(2)
	timeWindow := 10 * time.Second
	minRequests := int64(3)
	failureThreshold := 0.5

	t.Logf("📋 Configuration: OpenTimeout=%v, ThresholdHalfOpenReqs=%d, TimeWindow=%v, MinReqs=%d, FailureThreshold=%.1f%%",
		openTimeout, thresholdRequestsInHalfOpen, timeWindow, minRequests, failureThreshold*100)

	cb := NewCircuitBreaker("test-open-to-half-open",
		options.WithTimeWindow(timeWindow),
		options.WithMinimumRequestThreshold(minRequests),
		options.WithFailureRateThreshold(failureThreshold),
		options.WithOpenTimeout(openTimeout),
		options.WithThresholdRequestInHalfOpen(thresholdRequestsInHalfOpen),
		options.WithTargetRateInHalfOpen(1.0), // Require 100% success rate to close
	)
	t.Logf("✅ Circuit breaker created with key: %s", cb.GetKey())

	ctx := context.Background()

	// Step 1: Open the circuit
	t.Logf("🧪 PHASE 1: Opening circuit with failures")
	openingStart := time.Now()
	initialState := cb.GetState()
	t.Logf("📊 Initial state: %s", initialState)

	failureCount := 0
	for i := range 5 {
		requestStart := time.Now()
		t.Logf("📤 Executing failing request %d/5", i+1)

		err := cb.Execute(ctx, func(ctx context.Context) error {
			return errors.New("test error")
		})

		requestDuration := time.Since(requestStart)
		if err == nil {
			t.Errorf("❌ Request %d unexpectedly succeeded (duration: %v)", i+1, requestDuration)
		} else {
			failureCount++
			t.Logf("✅ Request %d failed as expected: %v (duration: %v)", i+1, err, requestDuration)
		}

		// Check state progression
		currentState := cb.GetState()
		totalReqs, successes, failures, failRate := cb.GetStatistics()
		t.Logf("📊 After request %d: State=%s, Stats(T:%d,S:%d,F:%d,R:%.1f%%)",
			i+1, currentState, totalReqs, successes, failures, failRate*100)
	}
	openingDuration := time.Since(openingStart)

	// Verify circuit is open
	stateAfterFailures := cb.GetState()
	if stateAfterFailures != StateOpen {
		t.Errorf("❌ Expected state to be OPEN, got %s", stateAfterFailures)
	} else {
		t.Logf("✅ Circuit opened successfully: %s (opened in %v with %d failures)", stateAfterFailures, openingDuration, failureCount)
	}

	// Step 2: Wait for timeout period (but don't expect state change yet)
	t.Logf("🧪 PHASE 2: Waiting for timeout period")
	t.Logf("⏰ Waiting for timeout period: %v", openTimeout)
	waitStart := time.Now()

	// Wait for the open timeout duration
	time.Sleep(openTimeout)

	waitDuration := time.Since(waitStart)
	t.Logf("⏰ Waited %v for timeout period (target: %v)", waitDuration, openTimeout)

	// Circuit should still be OPEN because no request has come in yet
	stateAfterTimeout := cb.GetState()
	if stateAfterTimeout != StateOpen {
		t.Errorf("❌ Expected state to still be OPEN after timeout (no request yet), got %s", stateAfterTimeout)
	} else {
		t.Logf("✅ Circuit state after timeout (no request): %s - transition requires a request", stateAfterTimeout)
	}

	// Step 3: Send a request to trigger HALF_OPEN transition
	t.Logf("🧪 PHASE 3: Triggering HALF_OPEN transition")
	transitionStart := time.Now()

	t.Logf("📤 Sending first request to trigger HALF_OPEN transition")
	err := cb.Execute(ctx, func(ctx context.Context) error {
		return nil // Success
	})
	transitionDuration := time.Since(transitionStart)

	// The request should be allowed and circuit should transition to HALF_OPEN
	if err != nil {
		t.Errorf("❌ Expected successful execution (should transition to HALF_OPEN), got error: %v (duration: %v)", err, transitionDuration)
	} else {
		t.Logf("✅ First request succeeded, triggering transition (duration: %v)", transitionDuration)
	}

	// Verify circuit is now half-open
	stateAfterTransition := cb.GetState()
	if stateAfterTransition != StateHalfOpen {
		t.Errorf("❌ Expected state to be HALF_OPEN after request, got %s", stateAfterTransition)
	} else {
		t.Logf("✅ Circuit successfully transitioned to: %s", stateAfterTransition)
	}

	// Step 4: Test that additional requests are allowed in half-open state
	t.Logf("🧪 PHASE 4: Testing additional requests in HALF_OPEN state")
	halfOpenTestStart := time.Now()

	// Second request should also be allowed
	t.Logf("📤 Executing second request in half-open state")
	secondRequestStart := time.Now()
	err = cb.Execute(ctx, func(ctx context.Context) error {
		return nil // Success
	})
	secondRequestDuration := time.Since(secondRequestStart)

	if err != nil {
		t.Errorf("❌ Expected successful execution in half-open state, got error: %v (duration: %v)", err, secondRequestDuration)
	} else {
		t.Logf("✅ Second request in half-open state completed successfully (duration: %v)", secondRequestDuration)
	}

	halfOpenTestDuration := time.Since(halfOpenTestStart)

	// Verify circuit is now closed (successful requests in half-open)
	finalState := cb.GetState()
	if finalState != StateClosed {
		t.Errorf("❌ Expected state to be CLOSED after successful half-open requests, got %s", finalState)
	} else {
		t.Logf("✅ Circuit successfully closed after half-open recovery: %s", finalState)
	}

	// Final statistics summary
	totalReqs, successes, failures, finalFailRate := cb.GetStatistics()
	t.Logf("📊 Final Statistics: Total=%d, Success=%d, Failure=%d, FailureRate=%.1f%%",
		totalReqs, successes, failures, finalFailRate*100)

	// Performance summary
	totalTestDuration := time.Since(testStartTime)
	t.Logf("📈 Performance Summary:")
	t.Logf("📈   Circuit Opening: %v", openingDuration)
	t.Logf("📈   Timeout Wait: %v", waitDuration)
	t.Logf("📈   Transition: %v", transitionDuration)
	t.Logf("📈   Half-Open Testing: %v", halfOpenTestDuration)
	t.Logf("📈   Total Test: %v", totalTestDuration)

	t.Logf("🏁 TEST COMPLETE: State transition test completed successfully in %v", totalTestDuration)
}

func TestCircuitBreakerHalfOpenToOpenTransition(t *testing.T) {
	t.Logf("🔧 TEST START: HALF_OPEN → OPEN State Transition (Failure Recovery)")
	testStartTime := time.Now()

	// Create circuit breaker with short open timeout
	openTimeout := 2 * time.Second
	thresholdRequestsInHalfOpen := int64(2)
	timeWindow := 10 * time.Second
	minRequests := int64(3)
	failureThreshold := 0.5

	t.Logf("📋 Configuration: OpenTimeout=%v, ThresholdHalfOpenReqs=%d, TimeWindow=%v, MinReqs=%d, FailureThreshold=%.1f%%",
		openTimeout, thresholdRequestsInHalfOpen, timeWindow, minRequests, failureThreshold*100)

	cb := NewCircuitBreaker("test-half-open-to-open",
		options.WithTimeWindow(timeWindow),
		options.WithMinimumRequestThreshold(minRequests),
		options.WithFailureRateThreshold(failureThreshold),
		options.WithOpenTimeout(openTimeout),
		options.WithThresholdRequestInHalfOpen(thresholdRequestsInHalfOpen),
		options.WithTargetRateInHalfOpen(1.0), // Require 100% success rate to close, any failure will open
	)
	t.Logf("✅ Circuit breaker created with key: %s", cb.GetKey())

	ctx := context.Background()

	// Step 1: Open the circuit
	t.Logf("🧪 PHASE 1: Opening circuit with failures")
	openingStart := time.Now()

	failureCount := 0
	for i := range 5 {
		requestStart := time.Now()
		t.Logf("📤 Executing failing request %d/5", i+1)

		err := cb.Execute(ctx, func(ctx context.Context) error {
			return errors.New("test error")
		})

		requestDuration := time.Since(requestStart)
		if err == nil {
			t.Errorf("❌ Request %d unexpectedly succeeded (duration: %v)", i+1, requestDuration)
		} else {
			failureCount++
			t.Logf("✅ Request %d failed as expected: %v (duration: %v)", i+1, err, requestDuration)
		}

		// Track state progression
		currentState := cb.GetState()
		totalReqs, successes, failures, failRate := cb.GetStatistics()
		t.Logf("📊 After request %d: State=%s, Stats(T:%d,S:%d,F:%d,R:%.1f%%)",
			i+1, currentState, totalReqs, successes, failures, failRate*100)
	}

	openingDuration := time.Since(openingStart)
	stateAfterOpening := cb.GetState()

	if stateAfterOpening != StateOpen {
		t.Errorf("❌ Expected state to be OPEN, got %s", stateAfterOpening)
	} else {
		t.Logf("✅ Circuit opened successfully: %s (opened in %v with %d failures)", stateAfterOpening, openingDuration, failureCount)
	}

	// Step 2: Wait for timeout period
	t.Logf("🧪 PHASE 2: Waiting for timeout period")
	t.Logf("⏰ Waiting for timeout period: %v", openTimeout)
	waitStart := time.Now()

	time.Sleep(openTimeout)

	waitDuration := time.Since(waitStart)
	stateAfterWait := cb.GetState()

	if stateAfterWait != StateOpen {
		t.Errorf("❌ Expected state to still be OPEN after timeout (no request yet), got %s", stateAfterWait)
	} else {
		t.Logf("✅ Circuit state after timeout (no request): %s (waited %v)", stateAfterWait, waitDuration)
	}

	// Step 3: Send a request to trigger HALF_OPEN transition with failure
	t.Logf("🧪 PHASE 3: Triggering HALF_OPEN transition with failing request")
	transitionStart := time.Now()

	t.Logf("📤 Sending failing request to trigger HALF_OPEN transition")
	for range 6 {
		cb.Execute(ctx, func(ctx context.Context) error {
			return errors.New("test error")
		})
	}
	transitionDuration := time.Since(transitionStart)

	// Verify circuit is now half-open
	stateAfterTransition := cb.GetState()
	if stateAfterTransition != StateOpen {
		t.Errorf("❌ Expected state to be HALF_OPEN after request, got %s", stateAfterTransition)
	} else {
		t.Logf("✅ Circuit successfully transitioned to: %s", stateAfterTransition)
	}

	// Verify circuit is open again
	finalState := cb.GetState()
	if finalState != StateOpen {
		t.Errorf("❌ Expected state to be OPEN after failures in half-open, got %s", finalState)
	} else {
		t.Logf("✅ Circuit reopened successfully after half-open failures: %s", finalState)
	}

	// Final statistics
	totalReqs, successes, failures, finalFailRate := cb.GetStatistics()
	t.Logf("📊 Final Statistics: Total=%d, Success=%d, Failure=%d, FailureRate=%.1f%%",
		totalReqs, successes, failures, finalFailRate*100)

	// Performance summary
	totalTestDuration := time.Since(testStartTime)
	t.Logf("📈 Performance Summary:")
	t.Logf("📈   Circuit Opening: %v", openingDuration)
	t.Logf("📈   Timeout Wait: %v", waitDuration)
	t.Logf("📈   Transition: %v", transitionDuration)
	t.Logf("📈   Total Test: %v", totalTestDuration)

	t.Logf("🏁 TEST COMPLETE: HALF_OPEN → OPEN transition test completed in %v", totalTestDuration)
}

func TestCircuitBreakerMixedHalfOpenRequests(t *testing.T) {
	t.Logf("🔧 Testing mixed success/failure requests in HALF_OPEN state")

	openTimeout := 2 * time.Second
	maxRequestsInHalfOpen := int64(3)
	cb := NewCircuitBreaker("test-mixed-half-open",
		options.WithTimeWindow(10*time.Second),
		options.WithMinimumRequestThreshold(3),
		options.WithFailureRateThreshold(0.5),
		options.WithOpenTimeout(openTimeout),
		options.WithThresholdRequestInHalfOpen(maxRequestsInHalfOpen),
		options.WithTargetRateInHalfOpen(0.5),
		options.WithThresholdRequestInHalfOpen(3),
	)

	ctx := context.Background()

	// Step 1: Open the circuit
	t.Logf("🔧 Step 1: Opening circuit with failures")
	for i := range 5 {
		t.Logf("📤 Executing failing request %d/5", i+1)
		cb.Execute(ctx, func(ctx context.Context) error {
			return errors.New("test error")
		})
	}

	if cb.GetState() != StateOpen {
		t.Errorf("Expected state to be OPEN, got %s", cb.GetState())
	}
	t.Logf("📊 Circuit state after failures: %s", cb.GetState())

	// Step 2: Wait for timeout period
	t.Logf("🔧 Step 2: Waiting for timeout period")
	time.Sleep(openTimeout)

	if cb.GetState() != StateOpen {
		t.Errorf("Expected state to still be OPEN after timeout (no request yet), got %s", cb.GetState())
	}
	t.Logf("📊 Circuit state after timeout (no request): %s", cb.GetState())

	// Step 3: Send first request to trigger HALF_OPEN transition
	t.Logf("🔧 Step 3: Sending first request to trigger HALF_OPEN transition")
	err := cb.Execute(ctx, func(ctx context.Context) error {
		return nil // Success
	})
	if err != nil {
		t.Errorf("Expected successful execution, got error: %v", err)
	}
	t.Logf("✅ First request completed successfully")

	// Verify circuit is now half-open
	if cb.GetState() != StateHalfOpen {
		t.Errorf("Expected state to be HALF_OPEN after first request, got %s", cb.GetState())
	}
	t.Logf("📊 Circuit state after first request: %s", cb.GetState())

	// Step 4: Test mixed requests in half-open state
	t.Logf("🔧 Step 4: Testing mixed requests in HALF_OPEN state")

	// Second request: Failure
	t.Logf("📤 Executing failing request 2/3 in half-open state")
	err = cb.Execute(ctx, func(ctx context.Context) error {
		return errors.New("test error")
	})
	if err == nil {
		t.Error("Expected error, got nil")
	}
	t.Logf("✅ Second request failed as expected")

	// Circuit should still be in HALF_OPEN state after one failure
	if cb.GetState() != StateHalfOpen {
		t.Errorf("Expected state to still be HALF_OPEN after one failure, got %s", cb.GetState())
	}
	t.Logf("📊 Circuit state after second request: %s", cb.GetState())

	// Third request: Success
	t.Logf("📤 Executing successful request 3/3 in half-open state")
	err = cb.Execute(ctx, func(ctx context.Context) error {
		return nil
	})
	if err != nil {
		t.Errorf("Expected successful execution, got error: %v", err)
	}
	t.Logf("✅ Third request completed successfully")

	// Verify circuit is closed (all test requests completed)
	if cb.GetState() != StateClosed {
		t.Errorf("Expected state to be CLOSED after all test requests, got %s", cb.GetState())
	}
	t.Logf("📊 Circuit state after mixed half-open requests: %s", cb.GetState())
}

func TestCircuitBreakerThresholdBasedHalfOpen(t *testing.T) {
	t.Logf("🔧 Testing new threshold-based half-open logic")

	openTimeout := 2 * time.Second
	thresholdRequests := int64(10)
	targetRate := 0.6 // 60% success rate required

	cb := NewCircuitBreaker("test-threshold-half-open",
		options.WithTimeWindow(10*time.Second),
		options.WithMinimumRequestThreshold(3),
		options.WithFailureRateThreshold(0.5),
		options.WithOpenTimeout(openTimeout),
		options.WithThresholdRequestInHalfOpen(thresholdRequests),
		options.WithTargetRateInHalfOpen(targetRate),
	)

	ctx := context.Background()

	// Step 1: Open the circuit with failures
	t.Logf("🔧 Step 1: Opening circuit with failures")
	for i := range 5 {
		t.Logf("📤 Executing failing request %d/5", i+1)
		cb.Execute(ctx, func(ctx context.Context) error {
			return errors.New("test error")
		})
	}

	if cb.GetState() != StateOpen {
		t.Errorf("Expected state to be OPEN, got %s", cb.GetState())
	}
	t.Logf("📊 Circuit state after failures: %s", cb.GetState())

	// Step 2: Wait for timeout and trigger half-open
	t.Logf("🔧 Step 2: Waiting for timeout period")
	time.Sleep(openTimeout)

	// First request to trigger half-open
	err := cb.Execute(ctx, func(ctx context.Context) error {
		return nil // Success
	})
	if err != nil {
		t.Errorf("Expected successful execution, got error: %v", err)
	}

	if cb.GetState() != StateHalfOpen {
		t.Errorf("Expected state to be HALF_OPEN, got %s", cb.GetState())
	}
	t.Logf("📊 Circuit transitioned to: %s", cb.GetState())

	// Step 3: Test successful scenario - 7 success out of 10 requests (70% success rate)
	t.Logf("🔧 Step 3: Testing successful scenario (7/10 success = 70%% > 60%% target)")

	// Send 6 more successful requests (total 7 success including first one)
	for i := range 6 {
		err := cb.Execute(ctx, func(ctx context.Context) error {
			return nil
		})
		if err != nil {
			t.Errorf("Expected successful execution, got error: %v", err)
		}
		t.Logf("✅ Success request %d/6 completed", i+1)
	}

	// Send 3 failing requests (total 3 failures)
	for i := range 3 {
		err := cb.Execute(ctx, func(ctx context.Context) error {
			return errors.New("test error")
		})
		if err == nil {
			t.Error("Expected error, got nil")
		}
		t.Logf("❌ Failure request %d/3 completed", i+1)
	}

	// After 10 requests (7 success, 3 failures = 70% success rate), circuit should close
	if cb.GetState() != StateClosed {
		t.Errorf("Expected state to be CLOSED after meeting target rate, got %s", cb.GetState())
	}
	t.Logf("📊 Circuit closed with 70%% success rate: %s", cb.GetState())

	// Verify detailed statistics
	stats := cb.GetDetailedStatistics()
	if stats.ThresholdRequestInHalfOpen != thresholdRequests {
		t.Errorf("Expected threshold requests %d, got %d", thresholdRequests, stats.ThresholdRequestInHalfOpen)
	}
	if stats.TargetRateInHalfOpen != targetRate {
		t.Errorf("Expected target rate %.2f, got %.2f", targetRate, stats.TargetRateInHalfOpen)
	}
	t.Logf("✅ Detailed statistics verified")
}

func TestCircuitBreakerThresholdBasedHalfOpenFailure(t *testing.T) {
	t.Logf("🔧 Testing threshold-based half-open logic - failure scenario")

	openTimeout := 2 * time.Second
	thresholdRequests := int64(10)
	targetRate := 0.6 // 60% success rate required

	cb := NewCircuitBreaker("test-threshold-half-open-fail",
		options.WithTimeWindow(10*time.Second),
		options.WithMinimumRequestThreshold(3),
		options.WithFailureRateThreshold(0.5),
		options.WithOpenTimeout(openTimeout),
		options.WithThresholdRequestInHalfOpen(thresholdRequests),
		options.WithTargetRateInHalfOpen(targetRate),
	)

	ctx := context.Background()

	// Step 1: Open the circuit
	t.Logf("🔧 Step 1: Opening circuit with failures")
	for range 5 {
		cb.Execute(ctx, func(ctx context.Context) error {
			return errors.New("test error")
		})
	}

	// Step 2: Wait and trigger half-open
	time.Sleep(openTimeout)
	cb.Execute(ctx, func(ctx context.Context) error {
		return nil // First success
	})

	if cb.GetState() != StateHalfOpen {
		t.Errorf("Expected state to be HALF_OPEN, got %s", cb.GetState())
	}

	// Step 3: Test failure scenario - 4 success out of 10 requests (40% success rate < 60% target)
	t.Logf("🔧 Step 3: Testing failure scenario (4/10 success = 40%% < 60%% target)")

	// Send 3 more successful requests (total 4 success including first one)
	for range 3 {
		err := cb.Execute(ctx, func(ctx context.Context) error {
			return nil
		})
		if err != nil {
			t.Errorf("Expected successful execution, got error: %v", err)
		}
	}

	// Send 6 failing requests (total 6 failures)
	for range 6 {
		err := cb.Execute(ctx, func(ctx context.Context) error {
			return errors.New("test error")
		})
		if err == nil {
			t.Error("Expected error, got nil")
		}
	}

	// After 10 requests (4 success, 6 failures = 40% success rate), circuit should open
	if cb.GetState() != StateOpen {
		t.Errorf("Expected state to be OPEN after failing to meet target rate, got %s", cb.GetState())
	}
	t.Logf("📊 Circuit opened with 40%% success rate: %s", cb.GetState())
}

func TestCircuitBreakerCatastrophicFailureProtection(t *testing.T) {
	t.Logf("🔧 Testing catastrophic failure protection in half-open state")

	openTimeout := 2 * time.Second
	thresholdRequests := int64(10)
	targetRate := 0.6

	cb := NewCircuitBreaker("test-catastrophic-protection",
		options.WithTimeWindow(10*time.Second),
		options.WithMinimumRequestThreshold(3),
		options.WithFailureRateThreshold(0.5),
		options.WithOpenTimeout(openTimeout),
		options.WithThresholdRequestInHalfOpen(thresholdRequests),
		options.WithTargetRateInHalfOpen(targetRate),
	)

	ctx := context.Background()

	// Step 1: Open the circuit
	for range 5 {
		err := cb.Execute(ctx, func(ctx context.Context) error {
			return errors.New("test error")
		})

		if err == nil {
			t.Error("❌ Expected error, got nil")
		}
	}

	if cb.GetState() != StateOpen {
		t.Errorf("❌ Expected state to be OPEN, got %s", cb.GetState())
	}

	t.Logf("📊 Circuit state after failures: %s", cb.GetState())

	// Step 2: Wait and trigger half-open
	time.Sleep(openTimeout)
	cb.Execute(ctx, func(ctx context.Context) error {
		return nil // First success
	})

	if cb.GetState() != StateHalfOpen {
		t.Errorf("❌ Expected state to be HALF_OPEN, got %s", cb.GetState())
	}
	t.Logf("📊 Circuit state after half-open: %s", cb.GetState())

	// Step 3: Test catastrophic failure (80% failure rate in first few requests)
	t.Logf("🔧 Step 3: Testing catastrophic failure protection")

	// Send 4 failing requests out of 5 total (80% failure rate)
	for range 10 {
		err := cb.Execute(ctx, func(ctx context.Context) error {
			return errors.New("catastrophic error")
		})
		if err == nil {
			t.Error("❌ Expected error, got nil")
		}
	}

	// Circuit should open immediately due to catastrophic failure protection
	if cb.GetState() != StateOpen {
		t.Errorf("❌ Expected state to be OPEN due to catastrophic failure, got %s", cb.GetState())
	}
	t.Logf("📊 Circuit opened early due to catastrophic failure protection: %s", cb.GetState())
}

func TestCircuitBreakerHalfOpenRequestLimit(t *testing.T) {
	t.Logf("🔧 Testing HALF_OPEN request limit enforcement")

	openTimeout := 2 * time.Second
	thresholdRequestsInHalfOpen := int64(2)
	cb := NewCircuitBreaker("test-half-open-limit",
		options.WithTimeWindow(10*time.Second),
		options.WithMinimumRequestThreshold(3),
		options.WithFailureRateThreshold(0.5),
		options.WithOpenTimeout(openTimeout),
		options.WithThresholdRequestInHalfOpen(thresholdRequestsInHalfOpen),
		options.WithTargetRateInHalfOpen(1.0), // Require 100% success rate to close
	)

	ctx := context.Background()

	// Step 1: Open the circuit
	t.Logf("🔧 Step 1: Opening circuit with failures")
	for range 5 {
		cb.Execute(ctx, func(ctx context.Context) error {
			return errors.New("test error")
		})
	}

	if cb.GetState() != StateOpen {
		t.Errorf("Expected state to be OPEN, got %s", cb.GetState())
	}
	t.Logf("📊 Circuit state after failures: %s", cb.GetState())

	// Step 2: Wait for timeout period
	t.Logf("🔧 Step 2: Waiting for timeout period")
	time.Sleep(openTimeout)

	// Step 3: Send first request to trigger HALF_OPEN transition
	t.Logf("🔧 Step 3: Sending first request to trigger HALF_OPEN transition")
	err := cb.Execute(ctx, func(ctx context.Context) error {
		return nil // Success
	})
	if err != nil {
		t.Errorf("Expected successful execution, got error: %v", err)
	}

	if cb.GetState() != StateHalfOpen {
		t.Errorf("Expected state to be HALF_OPEN after first request, got %s", cb.GetState())
	}
	t.Logf("📊 Circuit state after first request: %s", cb.GetState())

	// Step 4: Send second request (should be allowed and complete the threshold)
	t.Logf("🔧 Step 4: Sending second request (should complete threshold and close circuit)")
	err = cb.Execute(ctx, func(ctx context.Context) error {
		return nil // Success
	})
	if err != nil {
		t.Errorf("Expected successful execution, got error: %v", err)
	}
	t.Logf("✅ Second request completed successfully")

	// Step 5: Circuit should now be CLOSED due to meeting threshold with 100% success rate
	if cb.GetState() != StateClosed {
		t.Errorf("Expected state to be CLOSED after meeting threshold with 100%% success, got %s", cb.GetState())
	}
	t.Logf("📊 Circuit state after meeting threshold: %s", cb.GetState())

	// Step 6: Additional requests should work normally in CLOSED state
	t.Logf("🔧 Step 6: Testing additional requests in CLOSED state")
	err = cb.Execute(ctx, func(ctx context.Context) error {
		return nil // Success
	})
	if err != nil {
		t.Errorf("Expected successful execution in CLOSED state, got error: %v", err)
	}
	t.Logf("✅ Additional request works correctly in CLOSED state")
}

func TestCircuitBreakerDetailedStateTiming(t *testing.T) {
	t.Logf("🔧 Testing detailed state timing with precise measurements")

	openTimeout := 3 * time.Second
	thresholdRequestsInHalfOpen := int64(1)
	cb := NewCircuitBreaker("test-detailed-timing",
		options.WithTimeWindow(10*time.Second),
		options.WithMinimumRequestThreshold(2),
		options.WithFailureRateThreshold(0.5),
		options.WithOpenTimeout(openTimeout),
		options.WithThresholdRequestInHalfOpen(thresholdRequestsInHalfOpen),
		options.WithTargetRateInHalfOpen(1.0), // Require 100% success rate to close
	)

	ctx := context.Background()

	// Step 1: Open the circuit
	t.Logf("🔧 Step 1: Opening circuit")
	openStart := time.Now()

	for i := 0; i < 3; i++ {
		cb.Execute(ctx, func(ctx context.Context) error {
			return errors.New("test error")
		})
	}

	openDuration := time.Since(openStart)
	t.Logf("✅ Circuit opened in %v", openDuration)

	if cb.GetState() != StateOpen {
		t.Errorf("Expected state to be OPEN, got %s", cb.GetState())
	}
	t.Logf("📊 Circuit state: %s", cb.GetState())

	// Step 2: Wait for timeout period
	t.Logf("🔧 Step 2: Waiting for timeout period")
	time.Sleep(openTimeout)

	if cb.GetState() != StateOpen {
		t.Errorf("Expected state to still be OPEN after timeout (no request yet), got %s", cb.GetState())
	}
	t.Logf("📊 Circuit state after timeout (no request): %s", cb.GetState())

	// Step 3: Send request to trigger HALF_OPEN transition
	t.Logf("🔧 Step 3: Sending request to trigger HALF_OPEN transition")
	halfOpenStart := time.Now()

	err := cb.Execute(ctx, func(ctx context.Context) error {
		return nil // Success
	})
	halfOpenDuration := time.Since(halfOpenStart)

	if err != nil {
		t.Errorf("Expected successful execution, got error: %v", err)
	}
	t.Logf("✅ Request completed in %v", halfOpenDuration)

	// Since thresholdRequestsInHalfOpen = 1, the circuit should transition directly to CLOSED
	// after the first successful request (which triggers HALF_OPEN briefly and then CLOSED)
	if cb.GetState() != StateClosed {
		t.Errorf("Expected state to be CLOSED after successful half-open request, got %s", cb.GetState())
	}
	t.Logf("📊 Circuit state after request: %s", cb.GetState())

	// Step 4: Verify the successful transition
	t.Logf("🔧 Step 4: Verifying successful HALF_OPEN to CLOSED transition")

	// The circuit should now be CLOSED after meeting the threshold with 100% success rate
	t.Logf("📊 Circuit state after successful request: %s", cb.GetState())

	t.Logf("📈 Total test duration: %v", time.Since(openStart))
}

func TestCircuitBreakerConcurrentHalfOpenRequests(t *testing.T) {
	t.Logf("🔧 TEST START: Concurrent Requests in HALF_OPEN State")
	testStartTime := time.Now()

	openTimeout := 2 * time.Second
	thresholdRequestsInHalfOpen := int64(5)
	timeWindow := 10 * time.Second
	minRequests := int64(3)
	failureThreshold := 0.5
	concurrentRequestCount := 5

	t.Logf("📋 Configuration:")
	t.Logf("📋   OpenTimeout: %v", openTimeout)
	t.Logf("📋   ThresholdHalfOpenRequests: %d", thresholdRequestsInHalfOpen)
	t.Logf("📋   TimeWindow: %v", timeWindow)
	t.Logf("📋   MinRequests: %d", minRequests)
	t.Logf("📋   FailureThreshold: %.1f%%", failureThreshold*100)
	t.Logf("📋   Concurrent Requests: %d", concurrentRequestCount)

	cb := NewCircuitBreaker("test-concurrent-half-open",
		options.WithTimeWindow(timeWindow),
		options.WithMinimumRequestThreshold(minRequests),
		options.WithFailureRateThreshold(failureThreshold),
		options.WithOpenTimeout(openTimeout),
		options.WithThresholdRequestInHalfOpen(thresholdRequestsInHalfOpen),
		options.WithTargetRateInHalfOpen(0.6), // Require 60% success rate to close
	)
	t.Logf("✅ Circuit breaker created with key: %s", cb.GetKey())

	ctx := context.Background()

	// Step 1: Open the circuit
	t.Logf("🧪 PHASE 1: Opening circuit")
	openingStart := time.Now()

	failureCount := 0
	for i := range 5 {
		requestStart := time.Now()
		t.Logf("📤 Opening request %d/5", i+1)

		err := cb.Execute(ctx, func(ctx context.Context) error {
			return errors.New("test error")
		})

		requestDuration := time.Since(requestStart)
		if err == nil {
			t.Errorf("❌ Request %d unexpectedly succeeded (duration: %v)", i+1, requestDuration)
		} else {
			failureCount++
			t.Logf("✅ Request %d failed as expected (duration: %v)", i+1, requestDuration)
		}

		// Show state after key requests
		if i == 2 || i == 4 {
			currentState := cb.GetState()
			totalReqs, successes, failures, failRate := cb.GetStatistics()
			t.Logf("📊 After request %d: State=%s, Stats(T:%d,S:%d,F:%d,R:%.1f%%)",
				i+1, currentState, totalReqs, successes, failures, failRate*100)
		}
	}

	openingDuration := time.Since(openingStart)
	stateAfterOpening := cb.GetState()

	if stateAfterOpening != StateOpen {
		t.Errorf("❌ Expected state to be OPEN, got %s", stateAfterOpening)
	} else {
		t.Logf("✅ Circuit opened successfully: %s (opened in %v)", stateAfterOpening, openingDuration)
	}

	// Step 2: Wait for timeout period
	t.Logf("🧪 PHASE 2: Waiting for timeout period")
	t.Logf("⏰ Waiting for timeout: %v", openTimeout)
	waitStart := time.Now()

	time.Sleep(openTimeout)

	waitDuration := time.Since(waitStart)
	stateAfterWait := cb.GetState()

	if stateAfterWait != StateOpen {
		t.Errorf("❌ Expected state to still be OPEN after timeout (no request yet), got %s", stateAfterWait)
	} else {
		t.Logf("✅ Circuit state after timeout: %s (waited %v)", stateAfterWait, waitDuration)
	}

	// Step 3: Send initial request to trigger HALF_OPEN transition
	t.Logf("🧪 PHASE 3: Triggering HALF_OPEN transition")
	transitionStart := time.Now()

	t.Logf("📤 Sending initial request to trigger HALF_OPEN transition")
	err := cb.Execute(ctx, func(ctx context.Context) error {
		return nil // Success
	})

	transitionDuration := time.Since(transitionStart)

	if err != nil {
		t.Errorf("❌ Expected successful execution, got error: %v (duration: %v)", err, transitionDuration)
	} else {
		t.Logf("✅ Initial request succeeded (duration: %v)", transitionDuration)
	}

	stateAfterTransition := cb.GetState()
	if stateAfterTransition != StateHalfOpen {
		t.Errorf("❌ Expected state to be HALF_OPEN after initial request, got %s", stateAfterTransition)
	} else {
		t.Logf("✅ Circuit transitioned to: %s", stateAfterTransition)
	}

	// Step 4: Test concurrent requests in half-open state
	t.Logf("🧪 PHASE 4: Testing concurrent requests in HALF_OPEN state")
	concurrentStart := time.Now()

	results := make(chan struct {
		id       int
		err      error
		duration time.Duration
	}, concurrentRequestCount)

	// Launch concurrent requests
	t.Logf("🚀 Launching %d concurrent requests", concurrentRequestCount)
	for i := 0; i < concurrentRequestCount; i++ {
		go func(id int) {
			requestStart := time.Now()
			t.Logf("📤 Launching concurrent request %d", id+1)

			err := cb.Execute(ctx, func(ctx context.Context) error {
				// Alternate between success and failure
				if id%2 == 0 {
					return nil // Success
				}
				return errors.New("test error") // Failure
			})

			requestDuration := time.Since(requestStart)
			results <- struct {
				id       int
				err      error
				duration time.Duration
			}{id, err, requestDuration}
		}(i)
	}

	// Collect results
	var concurrentSuccessCount int
	var concurrentFailureCount int
	var blockedCount int
	var totalConcurrentDuration time.Duration

	t.Logf("📊 Collecting concurrent request results...")
	for range concurrentRequestCount {
		result := <-results
		totalConcurrentDuration += result.duration

		switch result.err {
		case nil:
			concurrentSuccessCount++
			t.Logf("✅ Concurrent request %d: SUCCESS (duration: %v)", result.id+1, result.duration)
		case ErrCircuitBreakerOpen:
			blockedCount++
			t.Logf("🚫 Concurrent request %d: BLOCKED (duration: %v)", result.id+1, result.duration)
		default:
			concurrentFailureCount++
			t.Logf("❌ Concurrent request %d: FAILURE - %v (duration: %v)", result.id+1, result.err, result.duration)
		}

	}

	concurrentDuration := time.Since(concurrentStart)
	avgRequestDuration := totalConcurrentDuration / time.Duration(concurrentRequestCount)
	finalState := cb.GetState()

	// Final statistics
	totalReqs, successes, failures, finalFailRate := cb.GetStatistics()

	t.Logf("📊 Concurrent Test Results:")
	t.Logf("📊   Total Concurrent Requests: %d", concurrentRequestCount)
	t.Logf("📊   Successful: %d", concurrentSuccessCount)
	t.Logf("📊   Failed: %d", concurrentFailureCount)
	t.Logf("📊   Blocked: %d", blockedCount)
	t.Logf("📊   Success Rate: %.1f%%", float64(concurrentSuccessCount)/float64(concurrentRequestCount)*100)
	t.Logf("📊   Average Request Duration: %v", avgRequestDuration)
	t.Logf("📊   Final Circuit State: %s", finalState)

	t.Logf("📊 Final Circuit Statistics:")
	t.Logf("📊   Total Requests: %d", totalReqs)
	t.Logf("📊   Successes: %d", successes)
	t.Logf("📊   Failures: %d", failures)
	t.Logf("📊   Failure Rate: %.1f%%", finalFailRate*100)

	// Performance summary
	testDuration := time.Since(testStartTime)
	t.Logf("📈 Performance Summary:")
	t.Logf("📈   Circuit Opening: %v", openingDuration)
	t.Logf("📈   Timeout Wait: %v", waitDuration)
	t.Logf("📈   Transition: %v", transitionDuration)
	t.Logf("📈   Concurrent Execution: %v", concurrentDuration)
	t.Logf("📈   Total Test: %v", testDuration)

	t.Logf("🏁 TEST COMPLETE: Concurrent half-open requests test completed in %v", testDuration)
}

// ============================================================================
// HTTP ENDPOINT INTEGRATION TESTS (Copy-Paste Ready for Terminal Testing)
// ============================================================================

/*
QUICK CIRCUIT BREAKER VALIDATION TEST (Copy this to terminal):

echo "=== QUICK CIRCUIT BREAKER VALIDATION ==="
BASE_URL="http://localhost:8080"
echo "1. Testing circuit breaker status endpoint..."
curl -s "$BASE_URL/circuit-breaker/status" | jq '.' || echo "❌ Status endpoint failed"
echo "2. Testing success endpoint..."
curl -s "$BASE_URL/api/success" | jq '.status' || echo "❌ Success endpoint failed"
echo "3. Opening circuit with failures..."
for i in {1..12}; do curl -s "$BASE_URL/api/server-error" > /dev/null 2>&1; done
echo "4. Checking if circuit opened..."
state=$(curl -s "$BASE_URL/circuit-breaker/status" | jq -r '.["GET--api-server-error"].state // "NOT_FOUND"')
echo "Circuit state: $state"
if [ "$state" = "OPEN" ]; then echo "✅ Circuit breaker is working correctly!"; else echo "❌ Circuit breaker may not be working (state: $state)"; fi
echo "=== QUICK VALIDATION COMPLETE ==="

*/

/*
BASIC FUNCTIONALITY TESTS (Copy each section separately):

# Test 1: Always Success Endpoint
echo "=== Testing Always Success Endpoint ==="
for i in {1..10}; do echo "Request $i:"; curl -s -w "Status: %{http_code}, Time: %{time_total}s\n" http://localhost:8080/api/success; done
echo "Checking circuit breaker status:"
curl -s http://localhost:8080/circuit-breaker/status | jq '.["GET--api-success"]'

# Test 2: Always Error Endpoint
echo "=== Testing Always Error Endpoint ==="
for i in {1..15}; do echo "Request $i:"; response=$(curl -s -w "Status: %{http_code}\n" http://localhost:8080/api/server-error); echo "$response"; sleep 0.5; done
echo "Final circuit status:"
curl -s http://localhost:8080/circuit-breaker/status | jq '.["GET--api-server-error"]'

# Test 3: Flaky Endpoint (50% failure rate)
echo "=== Testing Flaky Endpoint ==="
success_count=0; failure_count=0
for i in {1..20}; do response=$(curl -s -w "%{http_code}" http://localhost:8080/api/flaky); if [[ "$response" == *"200"* ]]; then success_count=$((success_count + 1)); echo "Request $i: SUCCESS"; else failure_count=$((failure_count + 1)); echo "Request $i: FAILURE"; fi; sleep 1; done
echo "Results: Success=$success_count, Failures=$failure_count"
echo "Circuit status:"
curl -s http://localhost:8080/circuit-breaker/status | jq '.["GET--api-flaky"]'

*/

/*
TIMEOUT AND SLOW RESPONSE TESTS:

# Test 4: Slow Endpoint Test
echo "=== Testing Slow Endpoint ==="
for i in {1..5}; do echo "Slow request $i (will take 8-12 seconds):"; start_time=$(date +%s); response=$(curl -s --max-time 15 -w "Status: %{http_code}, Time: %{time_total}s\n" http://localhost:8080/api/slow); end_time=$(date +%s); echo "$response"; echo "Actual time: $((end_time - start_time))s"; sleep 2; done
curl -s http://localhost:8080/circuit-breaker/status | jq '.["GET--api-slow"]'

# Test 5: Timeout Endpoint Test
echo "=== Testing Timeout Endpoint ==="
for i in {1..3}; do echo "Timeout test $i (will timeout):"; timeout 40 curl -s -w "Status: %{http_code}, Time: %{time_total}s\n" http://localhost:8080/api/timeout || echo "Request timed out as expected"; sleep 5; done
curl -s http://localhost:8080/circuit-breaker/status | jq '.["GET--api-timeout"]'

*/

/*
DATABASE SIMULATION TESTS:

# Test 6: Database Query Endpoint (30% failure rate)
echo "=== Testing Database Query Endpoint ==="
for i in {1..25}; do echo "DB Query $i:"; response=$(curl -s -w "Status: %{http_code}\n" http://localhost:8080/api/database/query); echo "$response"; sleep 0.8; done
echo "Database query circuit status:"
curl -s http://localhost:8080/circuit-breaker/status | jq '.["GET--api-database-query"]'

# Test 7: Database Insert Endpoint (20% failure rate)
echo "=== Testing Database Insert Endpoint ==="
for i in {1..20}; do echo "DB Insert $i:"; curl -s -X POST -w "Status: %{http_code}\n" http://localhost:8080/api/database/insert; sleep 1; done
curl -s http://localhost:8080/circuit-breaker/status | jq '.["POST--api-database-insert"]'

# Test 8: Database Connection Error (Always fails)
echo "=== Testing Database Connection Error ==="
for i in {1..10}; do echo "DB Connection test $i:"; curl -s -w "Status: %{http_code}\n" http://localhost:8080/api/database/connection-error; sleep 1; done
curl -s http://localhost:8080/circuit-breaker/status | jq '.["GET--api-database-connection-error"]'

*/

/*
EXTERNAL SERVICE SIMULATION TESTS:

# Test 9: Payment Service (25% failure rate)
echo "=== Testing External Payment Service ==="
for i in {1..20}; do echo "Payment request $i:"; response=$(curl -s -w "Status: %{http_code}\n" http://localhost:8080/api/external/payment); echo "$response"; sleep 1; done
curl -s http://localhost:8080/circuit-breaker/status | jq '.["GET--api-external-payment"]'

# Test 10: Notification Service (15% failure rate)
echo "=== Testing External Notification Service ==="
for i in {1..25}; do echo "Notification $i:"; curl -s -w "Status: %{http_code}\n" http://localhost:8080/api/external/notification; sleep 0.5; done
curl -s http://localhost:8080/circuit-breaker/status | jq '.["GET--api-external-notification"]'

# Test 11: Unreliable External Service (70% failure rate)
echo "=== Testing Unreliable External Service ==="
for i in {1..15}; do echo "Unreliable service request $i:"; curl -s -w "Status: %{http_code}\n" http://localhost:8080/api/external/unreliable; sleep 1; done
curl -s http://localhost:8080/circuit-breaker/status | jq '.["GET--api-external-unreliable"]'

*/

/*
DIFFERENT CIRCUIT BREAKER CONFIGURATION TESTS:

# Test 12: Aggressive Circuit Breaker (Opens quickly)
echo "=== Testing Aggressive Circuit Breaker ==="
echo "Configuration: 15s window, 3 min requests, 30% failure threshold, 10s timeout"
for i in {1..10}; do echo "Aggressive test $i:"; response=$(curl -s -w "Status: %{http_code}\n" http://localhost:8080/api/aggressive/test); echo "$response"; sleep 2; done
echo "Aggressive circuit status:"
curl -s http://localhost:8080/circuit-breaker/status | jq '.["GET--api-aggressive-test"]'

# Test 13: Conservative Circuit Breaker (More tolerant)
echo "=== Testing Conservative Circuit Breaker ==="
echo "Configuration: 2m window, 20 min requests, 80% failure threshold, 1m timeout"
for i in {1..30}; do echo "Conservative test $i:"; response=$(curl -s -w "Status: %{http_code}\n" http://localhost:8080/api/conservative/test); echo "$response"; sleep 1; done
echo "Conservative circuit status:"
curl -s http://localhost:8080/circuit-breaker/status | jq '.["GET--api-conservative-test"]'

*/

/*
PARTNER-SPECIFIC CIRCUIT BREAKER TESTS:

# Test 14: Stable Partner Tests
echo "=== Testing Stable Partner ==="
for i in {1..10}; do echo "Stable partner API test $i:"; curl -s -w "Status: %{http_code}\n" http://localhost:8080/partner/stable-partner/api/test; sleep 1; done
for i in {1..5}; do echo "Stable partner order $i:"; curl -s -X POST -w "Status: %{http_code}\n" http://localhost:8080/partner/stable-partner/api/order; sleep 1; done
echo "Stable partner circuit statuses:"
curl -s http://localhost:8080/circuit-breaker/status | jq 'to_entries | map(select(.key | contains("stable-partner")))'

# Test 15: Flaky Partner Tests
echo "=== Testing Flaky Partner ==="
for i in {1..15}; do echo "Flaky partner API test $i:"; curl -s -w "Status: %{http_code}\n" http://localhost:8080/partner/flaky-partner/api/test; sleep 1; done
for i in {1..10}; do echo "Flaky partner webhook $i:"; curl -s -X POST -w "Status: %{http_code}\n" http://localhost:8080/partner/flaky-partner/api/webhook; sleep 1; done
echo "Flaky partner circuit statuses:"
curl -s http://localhost:8080/circuit-breaker/status | jq 'to_entries | map(select(.key | contains("flaky-partner")))'

*/

/*
CRITICAL ENDPOINT TESTS:

# Test 16: Critical Payment Endpoint
echo "=== Testing Critical Payment Endpoint ==="
echo "Configuration: 30s window, 5 min requests, 20% failure threshold, 2m timeout"
for i in {1..20}; do echo "Critical payment $i:"; response=$(curl -s -X POST -w "Status: %{http_code}\n" http://localhost:8080/api/critical/payment); echo "$response"; sleep 2; done
curl -s http://localhost:8080/circuit-breaker/status | jq '.["POST--api-critical-payment"]'

# Test 17: Critical Order Endpoint
echo "=== Testing Critical Order Endpoint ==="
for i in {1..15}; do echo "Critical order $i:"; response=$(curl -s -X POST -w "Status: %{http_code}\n" http://localhost:8080/api/critical/order); echo "$response"; sleep 2; done
curl -s http://localhost:8080/circuit-breaker/status | jq '.["POST--api-critical-order"]'

*/

/*
LOAD TESTING SCENARIOS:

# Test 18: Burst Load Test
echo "=== Testing Burst Load Handling ==="
for size in 5 10 20 50; do echo "Testing burst size: $size"; for i in {1..10}; do curl -s "http://localhost:8080/api/load/burst?size=$size" > /dev/null 2>&1 & done; wait; sleep 2; done
curl -s http://localhost:8080/circuit-breaker/status | jq '.["GET--api-load-burst"]'

# Test 19: Sustained Load Test
echo "=== Testing Sustained Load ==="
for duration in 30 60 120 300; do echo "Testing sustained load for ${duration}s"; for i in {1..5}; do curl -s "http://localhost:8080/api/load/sustained?duration=$duration" > /dev/null 2>&1 & done; wait; sleep 5; done
curl -s http://localhost:8080/circuit-breaker/status | jq '.["GET--api-load-sustained"]'

# Test 20: Memory Intensive Operations
echo "=== Testing Memory Intensive Operations ==="
for size in 1000 2000 5000 10000; do echo "Testing memory size: ${size}KB"; response=$(curl -s "http://localhost:8080/api/load/memory?size=$size"); echo "$response"; sleep 2; done
curl -s http://localhost:8080/circuit-breaker/status | jq '.["GET--api-load-memory"]'

*/

/*
ERROR PATTERN SIMULATION TESTS:

# Test 21: Random Error Types
echo "=== Testing Random Error Types ==="
for i in {1..20}; do echo "Random error test $i:"; response=$(curl -s -w "Status: %{http_code}\n" http://localhost:8080/api/errors/random); echo "$response"; sleep 1; done
curl -s http://localhost:8080/circuit-breaker/status | jq '.["GET--api-errors-random"]'

# Test 22: Cascading Failures
echo "=== Testing Cascading Failures ==="
for step in {1..10}; do echo "Cascading failure step $step:"; response=$(curl -s "http://localhost:8080/api/errors/cascading?step=$step"); echo "$response"; sleep 1; done
curl -s http://localhost:8080/circuit-breaker/status | jq '.["GET--api-errors-cascading"]'

# Test 23: Intermittent Errors (Wave Pattern)
echo "=== Testing Intermittent Error Pattern ==="
echo "Pattern: 5 seconds of errors, 10 seconds of success"
for i in {1..60}; do echo "Intermittent test $i ($(date +%S)s):"; response=$(curl -s -w "Status: %{http_code}\n" http://localhost:8080/api/errors/intermittent); echo "$response"; sleep 1; done
curl -s http://localhost:8080/circuit-breaker/status | jq '.["GET--api-errors-intermittent"]'

*/

/*
CIRCUIT STATE TRANSITION TESTS:

# Test 24: Complete State Transitions (Closed → Open → Half-Open → Closed)
echo "=== Testing Complete State Transitions ==="
CB_KEY="GET--api-server-error"
BASE_URL="http://localhost:8080"
get_state() { curl -s "$BASE_URL/circuit-breaker/status" | jq -r ".\"$CB_KEY\".state // \"NOT_FOUND\""; }
echo "1. Initial state (should be CLOSED):"
curl -s "$BASE_URL/api/success" > /dev/null
initial_state=$(get_state)
echo "State: $initial_state"
echo "2. Opening circuit with failures..."
for i in {1..12}; do curl -s "$BASE_URL/api/server-error" > /dev/null; done
open_state=$(get_state)
echo "State after failures: $open_state"
echo "3. Verify circuit blocks requests (should return 503):"
blocked_response=$(curl -s -w "%{http_code}" "$BASE_URL/api/server-error")
echo "Response when blocked: $blocked_response"
echo "4. Waiting for half-open transition (30 seconds)..."
sleep 35
echo "5. Testing half-open state with success request:"
curl -s "$BASE_URL/api/success" > /dev/null
halfopen_state=$(get_state)
echo "State after success in half-open: $halfopen_state"
echo "=== State Transition Summary ==="
echo "Initial: $initial_state"
echo "After failures: $open_state"
echo "After recovery: $halfopen_state"

*/

/*
CIRCUIT BREAKER MANAGEMENT TESTS:

# Test 25: Manual Circuit Reset
echo "=== Testing Manual Circuit Reset ==="
echo "1. Opening circuit..."
for i in {1..10}; do curl -s http://localhost:8080/api/server-error > /dev/null; done
echo "2. Status before reset:"
curl -s http://localhost:8080/circuit-breaker/status | jq '.["GET--api-server-error"]'
echo "3. Resetting circuit breaker..."
reset_response=$(curl -s -X POST "http://localhost:8080/circuit-breaker/reset?key=GET--api-server-error")
echo "$reset_response"
echo "4. Status after reset:"
curl -s http://localhost:8080/circuit-breaker/status | jq '.["GET--api-server-error"]'
echo "5. Testing endpoint works after reset:"
curl -s http://localhost:8080/api/success

# Test 26: Reset All Circuit Breakers
echo "=== Testing Reset All Circuit Breakers ==="
echo "1. Opening multiple circuits..."
curl -s http://localhost:8080/api/server-error > /dev/null &
curl -s http://localhost:8080/api/external/unreliable > /dev/null &
curl -s http://localhost:8080/api/database/connection-error > /dev/null &
wait
echo "2. Status before reset all:"
curl -s http://localhost:8080/circuit-breaker/status | jq 'keys'
echo "3. Resetting all circuit breakers..."
reset_all_response=$(curl -s -X POST "http://localhost:8080/circuit-breaker/reset-all")
echo "$reset_all_response"
echo "4. Status after reset all:"
curl -s http://localhost:8080/circuit-breaker/status | jq 'to_entries | map({key: .key, state: .value.state})'

# Test 27: Invalid Reset Requests
echo "=== Testing Invalid Reset Requests ==="
echo "1. Reset with invalid key:"
curl -s -X POST "http://localhost:8080/circuit-breaker/reset?key=non-existent-key"
echo "2. Reset without key parameter:"
curl -s -X POST "http://localhost:8080/circuit-breaker/reset"
echo "3. Reset with empty key:"
curl -s -X POST "http://localhost:8080/circuit-breaker/reset?key="

*/

/*
HEALTH CHECK AND MONITORING TESTS:

# Test 28: Health Check (No Circuit Breaker Protection)
echo "=== Testing Health Check Bypass ==="
echo "Health check should always work, even with open circuits:"
for i in {1..10}; do echo "Health check $i:"; curl -s -w "Status: %{http_code}\n" http://localhost:8080/health; done
echo "Health checks should not appear in circuit breaker status:"
curl -s http://localhost:8080/circuit-breaker/status | jq 'keys | map(select(. | contains("health")))'

# Test 29: Metrics Endpoint
echo "=== Testing Metrics Endpoint ==="
for i in {1..5}; do echo "Metrics request $i:"; curl -s http://localhost:8080/metrics | jq '.circuit_breakers'; done

*/

/*
CONCURRENT LOAD TESTING:

# Test 30: Concurrent Success Requests
echo "=== Testing Concurrent Success Requests ==="
echo "Starting 10 concurrent processes with 5 requests each..."
for i in {1..10}; do { for j in {1..5}; do curl -s http://localhost:8080/api/success > /dev/null 2>&1; sleep 0.1; done } & done
wait
echo "Concurrent load completed."
echo "Circuit status after concurrent load:"
curl -s http://localhost:8080/circuit-breaker/status | jq '.["GET--api-success"]'

# Test 31: Concurrent Mixed Requests
echo "=== Testing Concurrent Mixed Requests ==="
echo "Starting mixed concurrent load..."
for i in {1..5}; do { for j in {1..10}; do if [ $((j % 3)) -eq 0 ]; then curl -s http://localhost:8080/api/server-error > /dev/null 2>&1; else curl -s http://localhost:8080/api/success > /dev/null 2>&1; fi; sleep 0.2; done } & done
wait
echo "Mixed concurrent load completed."
echo "Circuit statuses:"
curl -s http://localhost:8080/circuit-breaker/status | jq 'to_entries | map(select(.value.totalRequests > 0)) | map({key: .key, state: .value.state, requests: .value.totalRequests})'

*/

/*
PRODUCTION READINESS TEST SUITE:

echo "=== CIRCUIT BREAKER PRODUCTION READINESS TEST ==="
BASE_URL="http://localhost:8080"
PASSED=0
FAILED=0
run_test() { local test_name="$1"; local test_command="$2"; local expected_pattern="$3"; echo "Testing: $test_name"; result=$(eval "$test_command" 2>&1); if echo "$result" | grep -q "$expected_pattern"; then echo "✅ PASS: $test_name"; PASSED=$((PASSED + 1)); else echo "❌ FAIL: $test_name"; echo "   Result: $result"; FAILED=$((FAILED + 1)); fi; echo ""; }
run_test "Circuit Breaker Status Endpoint" "curl -s $BASE_URL/circuit-breaker/status" "{"
run_test "Health Check Always Works" "curl -s -w '%{http_code}' $BASE_URL/health" "200"
run_test "Success Endpoint Works" "curl -s -w '%{http_code}' $BASE_URL/api/success" "200"
echo "Opening circuit with failures..."
for i in {1..12}; do curl -s "$BASE_URL/api/server-error" > /dev/null 2>&1; done
run_test "Circuit Opens After Failures" "curl -s $BASE_URL/circuit-breaker/status | jq -r '.\"GET--api-server-error\".state'" "OPEN"
run_test "Open Circuit Blocks Requests" "curl -s -w '%{http_code}' $BASE_URL/api/server-error" "503"
run_test "Circuit Reset Works" "curl -s -X POST '$BASE_URL/circuit-breaker/reset?key=GET--api-server-error' | jq -r '.message'" "reset"
run_test "Partner Circuit Isolation" "curl -s $BASE_URL/partner/stable-partner/api/test && curl -s $BASE_URL/circuit-breaker/status | jq 'keys | length'" "[0-9]"
echo "=== TEST RESULTS ==="
echo "✅ Passed: $PASSED"
echo "❌ Failed: $FAILED"
echo "Total: $((PASSED + FAILED))"
if [ $FAILED -eq 0 ]; then echo "🎉 ALL TESTS PASSED - Production Ready!"; exit 0; else echo "🚨 Some tests failed - Review required before production"; exit 1; fi

*/

/*
MONITORING AND TROUBLESHOOTING COMMANDS:

# Real-time Circuit Breaker Monitoring
watch -n 2 'curl -s http://localhost:8080/circuit-breaker/status | jq -r "to_entries[] | \"\(.key): \(.value.state) (R:\(.value.totalRequests) F:\(.value.failureCount) Rate:\(.value.failureRate))\""'

# Detailed Circuit Analysis
curl -s http://localhost:8080/circuit-breaker/status | jq -r 'to_entries[] | "🔧 Circuit: \(.key)", "   State: \(.value.state)", "   Requests: \(.value.totalRequests)", "   Success: \(.value.successCount)", "   Failures: \(.value.failureCount)", "   Failure Rate: \(.value.failureRate)", "   Description: \(.value.description)", ""'

# Performance Impact Assessment
echo "Measuring circuit breaker performance impact..."
health_time=$(curl -s -w "%{time_total}" http://localhost:8080/health -o /dev/null)
protected_time=$(curl -s -w "%{time_total}" http://localhost:8080/api/success -o /dev/null)
echo "Health endpoint (no CB): ${health_time}s"
echo "Protected endpoint (with CB): ${protected_time}s"
echo "Overhead: $(echo "$protected_time - $health_time" | bc)s"

*/
