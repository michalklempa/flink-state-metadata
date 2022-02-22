package com.michalklempa.flink.state.metadata;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MoveImpl extends Move.AbstractMove {
    private static final Logger LOG = LoggerFactory.getLogger(MoveImpl.class);

    protected Path sourcePath;
    protected Path destinationPath;

    public MoveImpl(final CheckpointMetadata savepoint, final String sourceUri, final String destinationUri) {
        super(savepoint, sourceUri, destinationUri);
        this.sourcePath = new Path(sourceUri);
        this.destinationPath = new Path(destinationUri);
    }

    public CheckpointMetadata convert() throws IOException {
        final List<MasterState> masterStates = new ArrayList<>(savepoint.getMasterStates());

        List<OperatorState> operatorStates = new ArrayList<>(savepoint.getOperatorStates().size());

        for (OperatorState _taskState : savepoint.getOperatorStates()) {
            // Add task state
            OperatorState taskState = new OperatorState(_taskState.getOperatorID(), _taskState.getParallelism(), _taskState.getMaxParallelism());
            operatorStates.add(taskState);
            LOG.debug("Add taskState with ID: {}", _taskState.getOperatorID());

            // Sub task states
            for (Map.Entry<Integer, OperatorSubtaskState> _entry : _taskState.getSubtaskStates().entrySet()) {
                int subtaskIndex = _entry.getKey();
                LOG.debug("Add subtaskIndex: {}", subtaskIndex);

                OperatorSubtaskState subtaskState = convertSubtaskState(_entry.getValue());
                taskState.putState(subtaskIndex, subtaskState);
            }
        }

        return new CheckpointMetadata(savepoint.getCheckpointId(), operatorStates, masterStates);
    }

    private OperatorSubtaskState convertSubtaskState(OperatorSubtaskState _operatorSubtaskState) throws IOException {
        // Duration field has been removed from SubtaskState, do not remove
        LOG.debug("Convert managedOperatorState: {}", _operatorSubtaskState.getManagedOperatorState());
        StateObjectCollection<OperatorStateHandle> managedOperatorState = convertOperatorStateHandle(_operatorSubtaskState.getManagedOperatorState());

        LOG.debug("Convert rawOperatorState: {}", _operatorSubtaskState.getRawOperatorState());
        StateObjectCollection<OperatorStateHandle> rawOperatorState = convertOperatorStateHandle(_operatorSubtaskState.getRawOperatorState());

        LOG.debug("Convert managedKeyedState: {}", _operatorSubtaskState.getManagedKeyedState());
        StateObjectCollection<KeyedStateHandle> managedKeyedState = convertKeyedStateHandle(_operatorSubtaskState.getManagedKeyedState());

        LOG.debug("Convert rawKeyedState: {}", _operatorSubtaskState.getRawKeyedState());
        StateObjectCollection<KeyedStateHandle> rawKeyedState = convertKeyedStateHandle(_operatorSubtaskState.getRawKeyedState());

        return new OperatorSubtaskState(
                managedOperatorState,
                rawOperatorState,
                managedKeyedState,
                rawKeyedState);
    }

    private StateObjectCollection<OperatorStateHandle> convertOperatorStateHandle(StateObjectCollection<OperatorStateHandle> _operatorStateHandles) throws IOException {
        List<OperatorStateHandle> operatorStateHandles = new ArrayList<OperatorStateHandle>(_operatorStateHandles.size());
        for (OperatorStateHandle _operatorStateHandle : _operatorStateHandles) {
            Map<String, OperatorStateHandle.StateMetaInfo> offsetsMap = _operatorStateHandle.getStateNameToPartitionOffsets();
            StreamStateHandle stateHandle = convertStreamStateHandle(_operatorStateHandle.getDelegateStateHandle());
            OperatorStateHandle operatorStateHandle = new OperatorStreamStateHandle(offsetsMap, stateHandle);
            LOG.debug("Add OperatorStreamStateHandle: {}", operatorStateHandle);
            operatorStateHandles.add(operatorStateHandle);
        }
        return new StateObjectCollection<>(operatorStateHandles);
    }

    private StreamStateHandle convertStreamStateHandle(StreamStateHandle _streamStateHandle) throws IOException {
        if (_streamStateHandle instanceof FileStateHandle) {
            FileStateHandle _fileStateHandle = (FileStateHandle) _streamStateHandle;
            FileStateHandle fileStateHandle = new FileStateHandle(convertUri(_fileStateHandle.getFilePath()), _fileStateHandle.getStateSize());
            LOG.debug("Add FileStateHandle: {}", fileStateHandle);
            return fileStateHandle;
        } else if (_streamStateHandle instanceof ByteStreamStateHandle) {
            return _streamStateHandle;
        }
        throw new IllegalStateException("Unknown implementation of StreamStateHandle, class " + _streamStateHandle.getClass());
    }

    private StateObjectCollection<KeyedStateHandle> convertKeyedStateHandle(StateObjectCollection<KeyedStateHandle> _keyedStateHandles) throws IOException {
        List<KeyedStateHandle> keyedStateHandles = new ArrayList<KeyedStateHandle>(_keyedStateHandles.size());
        for (KeyedStateHandle _keyedStateHandle : _keyedStateHandles) {
            if (_keyedStateHandle instanceof KeyGroupsStateHandle) {
                KeyGroupsStateHandle _keyGroupsStateHandle = (KeyGroupsStateHandle) _keyedStateHandle;
                StreamStateHandle stateHandle = convertStreamStateHandle(_keyGroupsStateHandle.getDelegateStateHandle());
                KeyGroupsStateHandle keyGroupsStateHandle = new KeyGroupsStateHandle(_keyGroupsStateHandle.getGroupRangeOffsets(), stateHandle);
                LOG.debug("Add KeyGroupsStateHandle: {}", keyGroupsStateHandle);
                keyedStateHandles.add(keyGroupsStateHandle);
            } else if (_keyedStateHandle instanceof IncrementalRemoteKeyedStateHandle) {
                IncrementalRemoteKeyedStateHandle _incrementalRemoteKeyedStateHandle = (IncrementalRemoteKeyedStateHandle) _keyedStateHandle;
                LOG.debug("Convert MetaStateHandle: {}", _incrementalRemoteKeyedStateHandle.getMetaStateHandle());
                StreamStateHandle metaDataStateHandle = convertStreamStateHandle(_incrementalRemoteKeyedStateHandle.getMetaStateHandle());
                LOG.debug("Convert SharedState: {}", _incrementalRemoteKeyedStateHandle.getSharedState());
                Map<StateHandleID, StreamStateHandle> sharedStates = convertStreamStateHandleMap(_incrementalRemoteKeyedStateHandle.getSharedState());
                LOG.debug("Convert PrivateState: {}", _incrementalRemoteKeyedStateHandle.getPrivateState());
                Map<StateHandleID, StreamStateHandle> privateStates = convertStreamStateHandleMap(_incrementalRemoteKeyedStateHandle.getPrivateState());

                IncrementalRemoteKeyedStateHandle incrementalRemoteKeyedStateHandle =  new IncrementalRemoteKeyedStateHandle(
                        _incrementalRemoteKeyedStateHandle.getBackendIdentifier(),
                        _incrementalRemoteKeyedStateHandle.getKeyGroupRange(),
                        _incrementalRemoteKeyedStateHandle.getCheckpointId(),
                        sharedStates,
                        privateStates,
                        metaDataStateHandle);
                LOG.debug("Add IncrementalRemoteKeyedStateHandle: {}", incrementalRemoteKeyedStateHandle);
                keyedStateHandles.add(incrementalRemoteKeyedStateHandle);
            } else {
                throw new IllegalStateException("Unknown implementation of KeyedStateHandle, class: " + _keyedStateHandles.getClass());
            }
        }
        return new StateObjectCollection<>(keyedStateHandles);
    }

    private Map<StateHandleID, StreamStateHandle> convertStreamStateHandleMap(Map<StateHandleID, StreamStateHandle> _streamStateHandleMap) throws IOException {
        Map<StateHandleID, StreamStateHandle> streamStateHandleMap = new HashMap<>(_streamStateHandleMap.size());
        for (Map.Entry<StateHandleID, StreamStateHandle> _streamStateHandleMapEntry : _streamStateHandleMap.entrySet()) {
            LOG.debug("Convert StateHandleMap Entry: {}, {}", _streamStateHandleMapEntry.getKey(), _streamStateHandleMapEntry.getValue());
            StreamStateHandle stateHandle = convertStreamStateHandle(_streamStateHandleMapEntry.getValue());

            LOG.debug("Add StateHandleMap Entry: {}, {}", _streamStateHandleMapEntry.getKey(), stateHandle);
            streamStateHandleMap.put(_streamStateHandleMapEntry.getKey(), stateHandle);
        }
        return streamStateHandleMap;
    }

    private Path convertUri(Path _path) {
        if (_path.equals(sourcePath)) {
            LOG.info("Convert URI: {} to URI: {}", _path, destinationPath);
            return destinationPath;
        } else if (_path.getParent().equals(sourcePath)) {
            LOG.info("Convert URI: {} to URI: {}", _path, new Path(destinationPath, new Path(_path.getName())));
            return new Path(destinationPath, new Path(_path.getName()));
        }
        throw new IllegalStateException("Unknown path " + _path.getPath() + " - does not match source: " + sourceUri);
    }
}
