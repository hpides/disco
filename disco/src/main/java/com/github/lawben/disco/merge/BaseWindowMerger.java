package com.github.lawben.disco.merge;

import static com.github.lawben.disco.aggregation.FunctionWindowAggregateId.NO_CHILD_ID;

import com.github.lawben.disco.DistributedUtils;
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
import java.util.Comparator;
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

    protected final Set<Long> sessionWindows;
    protected final Map<WindowFunctionKey, List<DistributedAggregateWindowState<AggType>>> currentSessions;
    protected final Map<Integer, Map<WindowFunctionKey, Long>> childLastSessionStarts;
    protected final Map<WindowFunctionKey, Integer> lastReceivedChild;
    protected final Map<WindowFunctionKey, List<FunctionWindowAggregateId>> newSessionStarts;
    private Map<WindowFunctionKey, Long> sessionEnds;
    private Map<WindowFunctionKey, FunctionWindowAggregateId> lastSessionStart;

    public BaseWindowMerger(int numInputs, List<Window> windows, List<AggregateFunction> aggFunctions) {
        this.numInputs = numInputs;
        this.stateFactory = new MemoryStateFactory();
        this.aggFunctions = aggFunctions;
        this.receivedWindows = new HashMap<>();
        this.windowAggregates = new HashMap<>();

        this.sessionWindows = this.prepareSessionWindows(windows);
        this.currentSessions = new HashMap<>();
        this.childLastSessionStarts = new HashMap<>();
        this.lastReceivedChild = new HashMap<>();
        this.newSessionStarts = new HashMap<>();
        this.sessionEnds = new HashMap<>();
        this.lastSessionStart = new HashMap<>();
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

    protected Set<Long> prepareSessionWindows(List<Window> windows) {
        Set<Long> sessionWindows = new HashSet<>();
        for (Window window : windows) {
            if (window instanceof SessionWindow) {
                sessionWindows.add(window.getWindowId());
            }
        }
        return sessionWindows;
    }

    protected void processGlobalSession(AggType preAggregate, FunctionWindowAggregateId functionWindowAggId) {
        WindowFunctionKey windowFunctionKey = WindowFunctionKey.fromFunctionWindowId(functionWindowAggId);
        List<DistributedAggregateWindowState<AggType>> keyedStates =
                currentSessions.computeIfAbsent(windowFunctionKey, key -> new ArrayList<>());

        final int key = functionWindowAggId.getKey();
        final int childId = functionWindowAggId.getChildId();
        final long windowId = windowFunctionKey.getWindowId();
        WindowFunctionKey windowKey = new WindowFunctionKey(windowId, key);
        this.childLastSessionStarts.get(childId).put(windowKey, functionWindowAggId.getWindowId().getWindowStartTimestamp());

        DistributedAggregateWindowState<AggType> newSession = createNewSession(preAggregate, functionWindowAggId);
        if (keyedStates.isEmpty()) {
            // No sessions for this key exist. Create new session.
            keyedStates.add(newSession);
            lastReceivedChild.put(windowFunctionKey, childId);
            newSessionStarts.computeIfAbsent(windowKey, id -> new ArrayList<>()).add(functionWindowAggId);
            checkSessionWindowsTriggered(functionWindowAggId, keyedStates);
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
            newSessionStarts.get(windowKey).add(functionWindowAggId);
        }

        // Check every session to see if it has ended globally through the new session's child-start update.
        checkSessionWindowsTriggered(functionWindowAggId, keyedStates);
        lastReceivedChild.put(windowFunctionKey, childId);
    }

    private void checkSessionWindowsTriggered(FunctionWindowAggregateId functionWindowAggId,
            List<DistributedAggregateWindowState<AggType>> sessions) {
        int key = functionWindowAggId.getKey();
        int childId = functionWindowAggId.getChildId();
        long newStartTimestamp = functionWindowAggId.getWindowId().getWindowStartTimestamp();

        long lastSessionEnd = Long.MAX_VALUE;
        List<DistributedAggregateWindowState<AggType>> triggeredSession = new ArrayList<>();
        for (DistributedAggregateWindowState<AggType> session : sessions) {
            final boolean hasEnded = sessionHasGloballyEnded(session.getFunctionWindowId(), key, childId, newStartTimestamp);

            if (!hasEnded) {
                // If this session has not ended, newer sessions cannot have ended.
                break;
            }

            FunctionWindowAggregateId keylessId = functionWindowAggId.keylessCopy();
            Map<Integer, List<DistributedAggregateWindowState<AggType>>> keyedFinalStates =
                    windowAggregates.computeIfAbsent(keylessId, id -> new HashMap<>());
            List<DistributedAggregateWindowState<AggType>> finalStateList =
                    keyedFinalStates.computeIfAbsent(key, k -> new ArrayList<>());
            finalStateList.add(session);
            triggeredSession.add(session);
            lastSessionEnd = session.getWindowAggregateId().getWindowEndTimestamp();
        }

        // If we add long max_value again, we know that there is a session running and we should not send session
        // starts to our parent. Only add if there are active sessions
        if (!sessions.isEmpty()) {
            WindowFunctionKey windowKey = WindowFunctionKey.fromFunctionlessFunctionWindowId(functionWindowAggId);
            sessionEnds.put(windowKey, lastSessionEnd);
        }

        sessions.removeAll(triggeredSession);
    }

    private boolean sessionHasGloballyEnded(FunctionWindowAggregateId session,
            int key, int currentChildId, long currentStart) {
        final int oldSessionChildId = session.getChildId();
        final long sessionEnd = session.getWindowId().getWindowEndTimestamp();

        if (currentChildId != oldSessionChildId && currentStart < sessionEnd) {
            return false;
        }

        for (Map.Entry<Integer, Map<WindowFunctionKey, Long>> childAllStarts : childLastSessionStarts.entrySet()) {
            int childStartId = childAllStarts.getKey();
            if (childStartId == oldSessionChildId || childStartId == currentChildId) {
                // We can ignore the last child's session, because that is the one we are 'ending'.
                // The session of the calling child trivially ends the session.
                continue;
            }

            // If we have never seen a session for this key by a child, we assume it is still coming.
            WindowFunctionKey windowKey = new WindowFunctionKey(session.getWindowId().getWindowId(), key);
            long lastStartForChild = childAllStarts.getValue().getOrDefault(windowKey, -1L);
            if (lastStartForChild < sessionEnd) {
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
        long windowId = functionWindowAggId.getWindowId().getWindowId();
        return sessionWindows.contains(windowId);
    }

    @Override
    public Optional<FunctionWindowAggregateId> registerSessionStart(FunctionWindowAggregateId sessionStartId) {
        WindowFunctionKey windowKey = WindowFunctionKey.fromFunctionWindowId(sessionStartId);
        int currentChildId = sessionStartId.getChildId();
        long sessionStart = sessionStartId.getWindowId().getWindowStartTimestamp();

        // Add new session start for child.
        this.childLastSessionStarts.get(currentChildId).put(windowKey, sessionStart);

        // Check every session to see if it has ended globally through the new session's child-start update.
        List<DistributedAggregateWindowState<AggType>> keyedStates =
                currentSessions.computeIfAbsent(windowKey, k -> new ArrayList<>());
        checkSessionWindowsTriggered(sessionStartId, keyedStates);

        // Check if this gives us information about the minimum start of the next session.
        return getSessionStartFromChildStarts(windowKey);
    }

    public Optional<FunctionWindowAggregateId> getSessionStart(FunctionWindowAggregateId lastSession) {
        long windowId = lastSession.getWindowId().getWindowId();
        if (sessionWindows.isEmpty() || !sessionWindows.contains(windowId)) {
            // There are no session windows or this isn't a session, so we don't care about session starts
            return Optional.empty();
        }

        WindowFunctionKey windowKey = new WindowFunctionKey(windowId, lastSession.getKey());
        long lastSessionEnd = lastSession.getWindowId().getWindowEndTimestamp();

        List<FunctionWindowAggregateId> sessionStarts = newSessionStarts.getOrDefault(windowKey, new ArrayList<>());

        Optional<FunctionWindowAggregateId> explicitNewSession =
                DistributedUtils.getNextSessionStart(sessionStarts, lastSessionEnd);
        // If there are no explicit new sessions, check whether child starts give us enough information.
        Optional<FunctionWindowAggregateId> childStartNewSession = getSessionStartFromChildStarts(windowKey);

        final Optional<FunctionWindowAggregateId> newSession;
        if (!explicitNewSession.isPresent()) {
            newSession = childStartNewSession;
        } else if (!childStartNewSession.isPresent()) {
            newSession = explicitNewSession;
        } else {
            // Get earlier of one of both to guarantee correctness
            long explicitStart = explicitNewSession.get().getWindowId().getWindowStartTimestamp();
            long childStart = childStartNewSession.get().getWindowId().getWindowStartTimestamp();
            newSession = explicitStart < childStart ? explicitNewSession : childStartNewSession;
        }


        FunctionWindowAggregateId lastSessionForKey = lastSessionStart.get(windowKey);
        if (newSession.isPresent()) {
            lastSessionStart.put(windowKey, newSession.get());
            if (newSession.get().equals(lastSessionForKey)) {
                return Optional.empty();
            }
        }

        return newSession;
    }

    private Optional<FunctionWindowAggregateId> getSessionStartFromChildStarts(WindowFunctionKey windowKey) {
        final long lastSessionEndForKey = sessionEnds.getOrDefault(windowKey, -1L);
        List<DistributedAggregateWindowState<AggType>> sessionsForWindowKey = currentSessions.get(windowKey);
        if (sessionsForWindowKey != null && !sessionsForWindowKey.isEmpty() && lastSessionEndForKey == -1) {
            // There is a current session that has not been ended. Cannot send information to parent.
            return Optional.empty();
        }

        long minNewSessionStart = Long.MAX_VALUE;
        boolean allChildrenHaveNewerSessions = true;
        for (Map.Entry<Integer, Map<WindowFunctionKey, Long>> sessionStarts : childLastSessionStarts.entrySet()) {
            long sessionStartForKey = sessionStarts.getValue().getOrDefault(windowKey, Long.MIN_VALUE);
            if (sessionStartForKey <= lastSessionEndForKey) {
                allChildrenHaveNewerSessions = false;
                break;
            }
            minNewSessionStart = Math.min(minNewSessionStart, sessionStartForKey);
        }

        if (!allChildrenHaveNewerSessions) {
            return Optional.empty();
        }

        int key = windowKey.getKey();
        WindowAggregateId windowId =
                new WindowAggregateId(windowKey.getWindowId(), minNewSessionStart, minNewSessionStart);
        FunctionWindowAggregateId minNewSessionId = new FunctionWindowAggregateId(windowId, 0, NO_CHILD_ID, key);

        FunctionWindowAggregateId lastSessionForKey = lastSessionStart.get(windowKey);
        if (minNewSessionId.equals(lastSessionForKey)) {
            return Optional.empty();
        }

        lastSessionStart.put(windowKey, minNewSessionId);
        return Optional.of(minNewSessionId);
    }
}
