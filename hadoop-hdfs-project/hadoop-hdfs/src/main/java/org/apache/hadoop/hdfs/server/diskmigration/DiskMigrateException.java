
package org.apache.hadoop.hdfs.server.diskmigration;

import java.io.IOException;

/**
 * Created by lm140765 on 2017/7/22.
 */

/**
 * DiskMigration Exceptions.
 */
public class DiskMigrateException extends IOException {
  /**
   * Results returned by the RPC layer of DiskBalancer.
   */
  public enum Result {
    INVALID_MIGRATION,
    OLD_MIGRATION_SUBMITTED,
    MIGRATION_ALREADY_IN_PROGRESS,
    INVALID_VOLUME,
    INVALID_NEW_VOLUME,
    INTERNAL_ERROR,
    NO_SUCH_PLAN,
    INVALID_HOST_FILE_PATH,
    INVALID_NODE,
  }

  private final Result result;

  /**
   * Constructs an {@code IOException} with the specified detail message.
   *
   * @param message The detail message
   * @param result The returned exception type
   */
  public DiskMigrateException(String message, Result result) {
    super(message);
    this.result = result;
  }

  /**
   * Constructs an {@code IOException} with the specified detail message and
   * cause.
   *
   * @param message The detail message
   * @param cause   The cause (A null value is permitted, and
   *                indicates that the cause is nonexistent or unknown.)
   */
  public DiskMigrateException(String message, Throwable cause, Result result) {
    super(message, cause);
    this.result = result;
  }

  /**
   * Constructs an {@code IOException} with the specified cause and a detail
   * message
   *
   * @param cause The cause (A null value is permitted, and
   *              indicates that the cause is nonexistent or unknown.)
   */
  public DiskMigrateException(Throwable cause, Result result) {
    super(cause);
    this.result = result;
  }

  /**
   * Returns the result.
   * @return enum
   */
  public Result getResult() {
    return result;
  }
}
