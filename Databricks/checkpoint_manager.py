"""
checkpoint_manager.py — Track which batches have been processed

FIX: Handles missing checkpoint files gracefully on first run
Works with both dbutils (Databricks) and without (local testing)
"""

import json
from datetime import datetime, timezone


class CheckpointManager:

    def __init__(self, spark):
        self.spark = spark
        self.base = "s3://capstone-ecomm-team8/checkpoints"

    def _path(self, batch_number, stage):
        """Return the full S3 path for a checkpoint file."""
        return f"{self.base}/batch_{batch_number}/{stage}.json"

    def _file_exists(self, path):
        """Check if file exists using dbutils if available, otherwise assume not exists"""
        try:
            # Try using dbutils (Databricks)
            files = dbutils.fs.ls(path)
            return len(files) > 0
        except NameError:
            # dbutils not available (local mode or serverless without dbutils)
            # Try reading directly and catch exception
            try:
                df = self.spark.read.text(path)
                df.first()
                return True
            except:
                return False
        except Exception:
            # Path doesn't exist
            return False

    def is_done(self, batch_number, stage):
        """Check if this batch+stage has been completed"""
        path = self._path(batch_number, stage)

        # First check if file exists
        if not self._file_exists(path):
            print(f"    No checkpoint found for batch_{batch_number}/{stage} — will run.")
            return False

        try:
            df = self.spark.read.text(path)
            content = df.first()[0]
            data = json.loads(content)

            if data.get("status") == "completed":
                completed_at = data.get("completed_at", "unknown time")
                rows = data.get("rows", 0)

                print(f"    CHECKPOINT HIT: batch_{batch_number}/{stage}")
                print(f"    Already completed at {completed_at} with {rows:,} rows.")
                return True

        except Exception as e:
            print(f"    Checkpoint file exists but couldn't read it: {e}")
            return False

        return False

    def mark_done(self, batch_number, stage, rows=0):
        """Mark this batch+stage as completed"""
        path = self._path(batch_number, stage)
        
        data = {
            "batch": str(batch_number),
            "stage": stage,
            "status": "completed",
            "rows": rows,
            "completed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        
        content_df = self.spark.createDataFrame(
            [(json.dumps(data),)], ["value"]
        )
        
        content_df.coalesce(1).write.mode("overwrite").text(path)
        print(f"    Checkpoint saved: batch_{batch_number}/{stage} ({rows:,} rows)")

    def clear(self, batch_number, stage):
        """Delete a checkpoint to force re-run (alias for reset)"""
        self.reset(batch_number, stage)

    def reset(self, batch_number, stage="bronze"):
        """Delete a checkpoint to force re-run"""
        path = self._path(batch_number, stage)
        
        try:
            # Try dbutils first
            dbutils.fs.rm(path, recurse=True)
            print(f"  ✓ Checkpoint deleted: batch_{batch_number}/{stage}")
        except NameError:
            # dbutils not available
            print(f"  ⚠️ Cannot delete checkpoint - dbutils not available")
            print(f"     Manually delete: {path}")
        except Exception as e:
            # Checkpoint doesn't exist - that's OK
            print(f"  ⚠️ No checkpoint to delete for batch_{batch_number}/{stage} (OK)")

    def list_all(self):
        """Print summary of all checkpoints"""
        try:
            # Try dbutils
            batch_folders = dbutils.fs.ls(f"{self.base}/")
        except NameError:
            print("dbutils not available - cannot list checkpoints")
            return
        except Exception:
            print("No checkpoints found. Pipeline has not run yet.")
            return

        print(f"\n{'Batch':<10} {'Stage':<12} {'Status':<12} {'Rows':<12} {'Completed At'}")
        print("-" * 68)

        for folder in batch_folders:
            try:
                stage_files = dbutils.fs.ls(folder.path)
                for sf in stage_files:
                    if not sf.name.endswith(".json"):
                        continue
                    try:
                        content = self.spark.read.text(sf.path).first()[0]
                        data = json.loads(content)
                        print(
                            f"{data.get('batch','?'):<10} "
                            f"{data.get('stage','?'):<12} "
                            f"{data.get('status','?'):<12} "
                            f"{data.get('rows', 0):<12,} "
                            f"{data.get('completed_at','?')}"
                        )
                    except Exception:
                        pass
            except Exception:
                pass