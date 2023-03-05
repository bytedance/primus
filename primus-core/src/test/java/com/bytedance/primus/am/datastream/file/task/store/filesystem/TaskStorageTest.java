package com.bytedance.primus.am.datastream.file.task.store.filesystem;

import com.bytedance.primus.am.datastream.TaskWrapper;
import com.bytedance.primus.api.records.Task;
import com.bytedance.primus.api.records.impl.pb.TaskPBImpl;
import com.bytedance.primus.common.collections.Pair;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TaskStorageTest extends TaskStoreTestCommon {

  @Test
  public void testSerializeAndDeserializeTasks(@TempDir java.nio.file.Path dir) throws IOException {
    // Write
    List<Task> original = newTaskList(0, 4);
    try (FSDataOutputStream outputStream = fs.create(new Path(dir.toUri().getPath(), "tasks"))) {
      for (Task task : original) {
        task.write(outputStream);
      }
    }
    // Read
    List<Task> restored = new ArrayList<>();
    try (FSDataInputStream inputStream = fs.open(new Path(dir.toUri().getPath(), "tasks"))) {
      while (inputStream.available() > 0) {
        TaskPBImpl task = new TaskPBImpl();
        task.readFields(inputStream);
        restored.add(task);
      }
    }
    // Compare
    assertTaskListEquals(original, restored);
  }

  @Test
  public void testAddAndLoadNewPendingTasks(@TempDir java.nio.file.Path dir) throws IOException {
    // Init
    FileSystemTaskStore owner = newMockedFileSystemTaskStore(
        newMockedAMContext(new Path(dir.toUri().getPath()))
    );
    TaskStorage taskStorage = TaskStorage.create(owner);
    TaskVault taskVault = new TaskVault(owner, taskStorage);

    // Adding batch 0
    List<Task> batch0 = newTaskList(0, 4);
    Assertions.assertEquals(
        Optional.of(new Pair<>(0, batch0.get(3))),
        taskStorage.persistNewTasks(batch0)
    );
    taskStorage.loadNewTasksToVault(0, 0, taskVault);

    // Adding batch 1
    List<Task> batch1 = newTaskList(4, 4);
    Assertions.assertEquals(
        Optional.of(new Pair<>(1, batch1.get(3))),
        taskStorage.persistNewTasks(batch1)
    );
    taskStorage.loadNewTasksToVault(1, 0, taskVault);

    // Check pending tasks in TaskVault
    assertTaskListEquals(
        new ArrayList<Task>() {{
          addAll(batch0);
          addAll(batch1);
        }},
        taskVault.peekPendingTasks(100).stream()
            .map(TaskWrapper::getTask)
            .collect(Collectors.toList())
    );

    // Check again to ensure TaskVault is not mutated by peekPendingTasks
    assertTaskListEquals(
        new ArrayList<Task>() {{
          addAll(batch0);
          addAll(batch1);
        }},
        taskVault.peekPendingTasks(100).stream()
            .map(TaskWrapper::getTask)
            .collect(Collectors.toList())
    );
  }

  @Test
  public void testAddAndLoadNewPendingTasksWithOffset(@TempDir java.nio.file.Path dir)
      throws IOException {
    // Init
    FileSystemTaskStore owner = newMockedFileSystemTaskStore(
        newMockedAMContext(new Path(dir.toUri().getPath()))
    );
    TaskStorage taskStorage = TaskStorage.create(owner);
    TaskVault taskVault = new TaskVault(owner, taskStorage);

    // Adding batch 0
    List<Task> batch0 = newTaskList(0, 4);
    Assertions.assertEquals(
        Optional.of(new Pair<>(0, batch0.get(3))),
        taskStorage.persistNewTasks(batch0)
    );

    // Load from offset
    Pair<Integer, Long> coordinate = taskStorage.getFileIdAndPosition(3);
    taskStorage.loadNewTasksToVault(coordinate.getKey(), coordinate.getValue(), taskVault);

    // Check pending tasks in TaskVault
    assertTaskListEquals(
        batch0.subList(3, 4),
        taskVault.peekPendingTasks(100).stream()
            .map(TaskWrapper::getTask)
            .collect(Collectors.toList())
    );
  }
}
