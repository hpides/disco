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
            return;
        }

        final DistributedAggregateWindowState<AggType> latestSession = keyedStates.get(keyedStates.size() - 1);

        // Either a new session or one that will be merged.
        keyedStates.add(newSession);

        int newSessionPos = keyedStates.size() - 2;
        boolean wasMerged = false;
        for (int sessionPos = newSessionPos; sessionPos >= 0; sessionPos--) {
            DistributedAggregateWindowState<AggType> previousSession = keyedStates.get(sessionPos);
            DistributedAggregateWindowState<AggType> currentSession = keyedStates.get(sessionPos + 1);
            // New session 'extends' existing session. Merge them.
            if (sessionsOverlap(previousSession.getFunctionWindowId(), currentSession.getFunctionWindowId())) {
                DistributedAggregateWindowState<AggType> mergedState = mergeSessions(previousSession, currentSession);
                keyedStates.set(sessionPos, mergedState);
                newSessionPos = sessionPos;
                wasMerged = true;
            } else {
                break;
            }
        }

        if (wasMerged) {
            keyedStates.subList(newSessionPos + 1, keyedStates.size()).clear();
            lastReceivedChild.put(windowFunctionKey, childId);
            return;
        }

        // New session is new global session.
        // Check if this ends previous global session.
        boolean endsSession = true;
        for (Map.Entry<Integer, Map<WindowFunctionKey, Long>> childAllStarts : childLastSessionStarts.entrySet()) {
            Integer currentChildId = childAllStarts.getKey();
            int lastChild = lastReceivedChild.computeIfAbsent(windowFunctionKey, k -> childId);
            if (currentChildId.equals(lastChild) || currentChildId.equals(childId)) {
                // We can ignore the last child's session, because that is the one we are 'ending'.
                // The session of the calling child obviously ends the session.
                continue;
            }

            // If we have never seen at session for this key by a child, we assume it is still coming
            long lastStart = childAllStarts.getValue().getOrDefault(windowFunctionKey, -1L);
            if (lastStart < latestSession.getEnd()) {
                endsSession = false;
                break;
            }
        }

        if (endsSession) {
            // The current session triggers the previous session as every child has sent newer data and there will be
            // no more data for that old session.
            long currentStart = functionWindowAggId.getWindowId().getWindowStartTimestamp();
            for (DistributedAggregateWindowState<AggType> session : keyedStates) {
                if (session.getEnd() < currentStart) {
                    FunctionWindowAggregateId keylessId = functionWindowAggId.keylessCopy();
                    Map<Integer, List<DistributedAggregateWindowState<AggType>>> keyedFinalStates =
                            windowAggregates.computeIfAbsent(keylessId, id -> new HashMap<>());
                    List<DistributedAggregateWindowState<AggType>> finalStateList =
                            keyedFinalStates.computeIfAbsent(windowFunctionKey.getKey(), k -> new ArrayList<>());
                    finalStateList.add(session);
                } else {
                    break;
                }
            }
        }

        lastReceivedChild.put(windowFunctionKey, childId);
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
