package com.github.lawben.disco;

import static com.github.lawben.disco.Event.NO_KEY;

import com.github.lawben.disco.aggregation.DistributedAggregateWindowState;
import com.github.lawben.disco.aggregation.FunctionWindowAggregateId;
import com.github.lawben.disco.aggregation.WindowFunctionKey;
import de.tub.dima.scotty.core.WindowAggregateId;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.SessionWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.slicing.state.AggregateState;
import de.tub.dima.scotty.state.StateFactory;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

public abstract class BaseWindowMerger<AggType> implements WindowMerger<AggType> {
    protected int numInputs;
    protected final StateFactory stateFactory;
    protected final List<AggregateFunction> aggFunctions;
    protected final Map<FunctionWindowAggregateId, LongAdder> receivedWindows;
    protected final Map<FunctionWindowAggregateId,
            Map<Integer, List<DistributedAggregateWindowState<AggType>>>> windowAggregates;

    protected final Set<WindowFunctionKey> sessionWindows;
    protected final Map<WindowFunctionKey, List<DistributedAggregateWindowState<AggType>>> currentSessions;
    protected final Map<Integer, Map<WindowFunctionKey, Long>> childLastSessionStarts;
    protected final Map<WindowFunctionKey, Integer> lastReceivedChild;

    public BaseWindowMerger(int numInputs, List<Window> windows, List<AggregateFunction> aggFunctions) {
        this.numInputs = numInputs;
        this.stateFactory = new MemoryStateFactory();
        this.aggFunctions = aggFunctions;
        this.receivedWindows = new HashMap<>();
        this.windowAggregates = new HashMap<>();

        this.sessionWindows = this.prepareSessionWindows(windows, aggFunctions);
        this.currentSessions = new HashMap<>();
        this.childLastSessionStarts = new HashMap<>();
        this.lastReceivedChild = new HashMap<>();
    }

    public void initializeSessionState(List<Integer> childIds) {
        for (int childId : childIds) {
            childLastSessionStarts.putIfAbsent(childId, new HashMap<>());
        }
    }

    @Override
    public List<AggregateFunction> getAggregateFunctions() {
        return this.aggFunctions;
    }

    @Override
    public Optional<FunctionWindowAggregateId> checkWindowComplete(FunctionWindowAggregateId functionWindowAggId) {
        if (isSessionWindow(functionWindowAggId)) {
            FunctionWindowAggregateId keylessId = functionWindowAggId.keylessCopy();
            if (windowAggregates.getOrDefault(keylessId, new HashMap<>()).containsKey(functionWindowAggId.getKey())) {
                return Optional.of(functionWindowAggId);
            }

            return Optional.empty();
        }

        FunctionWindowAggregateId keylessId = functionWindowAggId.keylessCopy();
        LongAdder receivedCounter = receivedWindows.computeIfAbsent(keylessId, k -> new LongAdder());
        if (receivedCounter.longValue() == 0) {
            receivedCounter.add(numInputs);
        }
        receivedCounter.decrement();
        return receivedCounter.longValue() == 0 ? Optional.of(keylessId) : Optional.empty();
    }

    @Override
    public List<DistributedAggregateWindowState<AggType>> triggerFinalWindow(FunctionWindowAggregateId functionWindowId) {
        FunctionWindowAggregateId keylessId = functionWindowId.keylessCopy();
        Map<Integer, List<DistributedAggregateWindowState<AggType>>> keyedStates = windowAggregates.remove(keylessId);

        List<DistributedAggregateWindowState<AggType>> finalWindows = new ArrayList<>(keyedStates.size());
        for (Map.Entry<Integer, List<DistributedAggregateWindowState<AggType>>> keyedState : keyedStates.entrySet()) {
            int key = keyedState.getKey();

            for (DistributedAggregateWindowState<AggType> finalState : keyedState.getValue()) {
                AggregateState<AggType> state = finalState.getWindowState();
                FunctionWindowAggregateId stateId = finalState.getFunctionWindowId();
                FunctionWindowAggregateId keyedId = stateId.withKey(key);
                finalWindows.add(new DistributedAggregateWindowState<>(keyedId, state));
            }
        }

        receivedWindows.remove(keylessId);
        return finalWindows;
    }

    protected Set<WindowFunctionKey> prepareSessionWindows(List<Window> windows, List<AggregateFunction> aggregateFunctions) {
        Set<WindowFunctionKey> sessionWindows = new HashSet<>();
        for (Window window : windows) {
            if (window instanceof SessionWindow) {
                SessionWindow sw = (SessionWindow) window;
                long windowId = sw.getWindowId();
                for (int functionId = 0; functionId < aggregateFunctions.size(); functionId++) {
                    WindowFunctionKey windowFunctionKey = new WindowFunctionKey(windowId, functionId, NO_KEY);
                    sessionWindows.add(windowFunctionKey);
                }
            }
        }
        return sessionWindows;
    }

    protected void processGlobalSession(AggType preAggregate, FunctionWindowAggregateId functionWindowAggId) {
        WindowFunctionKey windowFunctionKey = WindowFunctionKey.fromFunctionWindowId(functionWindowAggId);
        List<DistributedAggregateWindowState<AggType>> keyedStates =
                currentSessions.computeIfAbsent(windowFunctionKey, key -> new ArrayList<>());

        final int childId = functionWindowAggId.getChildId();
        Map<WindowFunctionKey, Long> childStarts =
                childLastSessionStarts.computeIfAbsent(childId, id -> new HashMap<>());
        childStarts.put(windowFunctionKey, functionWindowAggId.getWindowId().getWindowStartTimestamp());

        DistributedAggregateWindowState<AggType> newSession = createNewSession(preAggregate, functionWindowAggId);
        if (keyedStates.isEmpty()) {
            // No sessions for this key exist. Create new session.
            keyedStates.add(newSession);
            lastReceivedChild.put(windowFunctionKey, childId);
            return;
        }

        // Either a new session or one that will be merged.
        final int numSessions = keyedStates.size();
        List<Integer> sessionPositionsToMerge = new ArrayList<>();
        for (int sessionPos = 0; sessionPos < numSessions; sessionPos++) {
            DistributedAggregateWindowState<AggType> currentSession = keyedStates.get(sessionPos);
            if (sessionsOverlap(currentSession.getFunctionWindowId(), newSession.getFunctionWindowId())) {
                sessionPositionsToMerge.add(sessionPos);
            }
        }

        if (!sessionPositionsToMerge.isEmpty()) {
            // New session merges existing sessions.
            List<DistributedAggregateWindowState<AggType>> mergedSessions = new ArrayList<>(numSessions);
            int minMergePos = sessionPositionsToMerge.get(0);
            int maxMergePos = sessionPositionsToMerge.get(sessionPositionsToMerge.size() - 1);

            // Copy all 'older' unmerged sessions
            for (int i = 0; i < minMergePos; i++) {
                mergedSessions.add(keyedStates.get(i));
            }

            // Merge overlapping sessions
            DistributedAggregateWindowState<AggType> mergedSession = newSession;
            for (int i = minMergePos; i < maxMergePos + 1; i++) {
                DistributedAggregateWindowState<AggType> oldSession = keyedStates.get(i);
                mergedSession = mergeSessions(oldSession, mergedSession);
            }
            FunctionWindowAggregateId idWithChild =
                    new FunctionWindowAggregateId(mergedSession.getFunctionWindowId(), childId);
            DistributedAggregateWindowState<AggType> sessionWithChildId =
                    new DistributedAggregateWindowState<>(idWithChild, mergedSession.getWindowState());
            mergedSessions.add(sessionWithChildId);

            // Copy all 'newer' unmerged sessions
            for (int i = maxMergePos + 1; i < numSessions; i++) {
                mergedSessions.add(keyedStates.get(i));
            }

            keyedStates.clear();
            keyedStates.addAll(mergedSessions);
        } else {
            // No merged happened. Just add new session.
            keyedStates.add(newSession);
        }

        // Check every session to see if it has ended globally through the new session's child-start update.
        List<DistributedAggregateWindowState<AggType>> triggeredSession = new ArrayList<>();
        for (DistributedAggregateWindowState<AggType> session : keyedStates) {
            final boolean hasEnded = sessionHasGloballyEnded(session.getFunctionWindowId(), windowFunctionKey,
                    childId, functionWindowAggId.getWindowId().getWindowStartTimestamp());

            if (hasEnded) {
                FunctionWindowAggregateId keylessId = functionWindowAggId.keylessCopy();
                Map<Integer, List<DistributedAggregateWindowState<AggType>>> keyedFinalStates =
                        windowAggregates.computeIfAbsent(keylessId, id -> new HashMap<>());
                List<DistributedAggregateWindowState<AggType>> finalStateList =
                        keyedFinalStates.computeIfAbsent(windowFunctionKey.getKey(), k -> new ArrayList<>());
                finalStateList.add(session);
                triggeredSession.add(session);
            } else {
                break;
            }
        }

        keyedStates.removeAll(triggeredSession);
        lastReceivedChild.put(windowFunctionKey, childId);
    }

    private boolean sessionHasGloballyEnded(FunctionWindowAggregateId session,
            WindowFunctionKey windowFunctionKey, int currentChildId, long currentStart) {
        final int oldSessionChildId = session.getChildId();
        final long sessionEnd = session.getWindowId().getWindowEndTimestamp();

        if (currentStart < sessionEnd) {
            return false;
        }

        for (Map.Entry<Integer, Map<WindowFunctionKey, Long>> childAllStarts : childLastSessionStarts.entrySet()) {
            Integer childStartId = childAllStarts.getKey();
            if (childStartId.equals(oldSessionChildId) || childStartId.equals(currentChildId)) {
                // We can ignore the last child's session, because that is the one we are 'ending'.
                // The session of the calling child obviously ends the session.
                continue;
            }

            // If we have never seen at session for this key by a child, we assume it is still coming.
            long lastStartForChild = childAllStarts.getValue().getOrDefault(windowFunctionKey, -1L);
            if (sessionEnd > lastStartForChild) {
                return false;
            }
        }
        return true;
    }

    protected DistributedAggregateWindowState<AggType> mergeSessions(
            DistributedAggregateWindowState<AggType> oldSession,
            DistributedAggregateWindowState<AggType> currentSession) {
        AggregateState<AggType> oldState = oldSession.getWindowState();
        AggregateState<AggType> currentState = currentSession.getWindowState();
        oldState.merge(currentState);

        WindowAggregateId oldId = oldSession.getFunctionWindowId().getWindowId();
        WindowAggregateId currentId = currentSession.getFunctionWindowId().getWindowId();

        long newStart = Math.min(oldId.getWindowStartTimestamp(), currentId.getWindowStartTimestamp());
        long newEnd = Math.max(oldId.getWindowEndTimestamp(), currentId.getWindowEndTimestamp());
        WindowAggregateId newWindow = new WindowAggregateId(oldId.getWindowId(), newStart, newEnd);
        FunctionWindowAggregateId newId =
                new FunctionWindowAggregateId(newWindow, oldSession.getFunctionWindowId().getFunctionId())
                        .withKey(oldSession.getFunctionWindowId().getKey());

        return new DistributedAggregateWindowState<>(newId, oldState);
    }

    private DistributedAggregateWindowState<AggType> createNewSession(AggType aggregate,
            FunctionWindowAggregateId functionWindowId) {
        AggregateState<AggType> state = new AggregateState<>(new MemoryStateFactory(), this.aggFunctions);
        state.addElement(aggregate);
        return new DistributedAggregateWindowState<>(functionWindowId, state);
    }

    private boolean sessionsOverlap(FunctionWindowAggregateId session1, FunctionWindowAggregateId session2) {
        // https://stackoverflow.com/questions/325933/determine-whether-two-date-ranges-overlap/325964#325964
        final long session1Start = session1.getWindowId().getWindowStartTimestamp();
        final long session1End = session1.getWindowId().getWindowEndTimestamp();
        final long session2Start = session2.getWindowId().getWindowStartTimestamp();
        final long session2End = session2.getWindowId().getWindowEndTimestamp();
        return session1Start <= session2End && session2Start <= session1End;
    }

    protected boolean isSessionWindow(FunctionWindowAggregateId functionWindowAggId) {
        FunctionWindowAggregateId keylessId = functionWindowAggId.keylessCopy();
        final WindowFunctionKey windowFunctionKey = WindowFunctionKey.fromFunctionWindowId(keylessId);
        return sessionWindows.contains(windowFunctionKey);
    }
}
