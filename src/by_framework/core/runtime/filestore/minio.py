"""
MinIO/S3 storage implementation.

Provides file storage backed by MinIO or S3-compatible object storage.
"""

from io import BytesIO

from .base import FileStorage


class MinioFileStorage(FileStorage):
    """MinIO/S3 storage implementation.

    Provides file storage backed by MinIO or S3-compatible object storage.
    Suitable for distributed deployments.

    Note: Requires minio package. Install with: pip install minio
    """

    def __init__(
        self,
        bucket: str,
        endpoint: str,
        access_key: str,
        secret_key: str,
        secure: bool = False,
        region: str = "us-east-1",
    ):
        """Initialize MinIO storage.

        Args:
            bucket: Storage bucket name
            endpoint: MinIO server endpoint (e.g., "minio.example.com:9000")
            access_key: Access key
            secret_key: Secret key
            secure: Whether to use HTTPS
            region: S3 region
        """
        # Lazy import to avoid hard dependency
        from minio import Minio
        from minio.error import S3Error

        self._minio_module = Minio
        self._s3_error = S3Error
        self.bucket = bucket
        self.region = region
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    async def initialize(self) -> None:
        """Initialize the storage backend."""
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket, self.region)

    async def shutdown(self) -> None:
        """Shutdown the storage backend."""
        pass  # MinIO SDK doesn't require explicit shutdown

    async def write(self, path: str, content: str | bytes, encoding: str = "utf-8") -> None:
        """Write content to a file.

        Args:
            path: File path (relative to workspace)
            content: File content
            encoding: Text encoding (only effective for str type)
        """
        data = content if isinstance(content, bytes) else content.encode(encoding)
        data_size = len(data)
        self.client.put_object(
            self.bucket,
            path,
            BytesIO(data),
            data_size,
        )

    async def read(self, path: str, encoding: str = "utf-8") -> str | bytes:
        """Read file content.

        Args:
            path: File path (relative to workspace)
            encoding: Text encoding

        Returns:
            File content (str or bytes)
        """
        response = self.client.get_object(self.bucket, path)
        try:
            data = response.read()
            if encoding:
                return data.decode(encoding)
            return data
        finally:
            response.close()
            response.release_conn()

    async def delete(self, path: str) -> None:
        """Delete a file.

        Args:
            path: File path (relative to workspace)
        """
        try:
            self.client.stat_object(self.bucket, path)
            self.client.remove_object(self.bucket, path)
        except self._s3_error:
            pass  # Ignore if file doesn't exist

    async def exists(self, path: str) -> bool:
        """Check if path exists.

        Args:
            path: File path (relative to workspace)

        Returns:
            True if exists, False otherwise
        """
        try:
            self.client.stat_object(self.bucket, path)
            return True
        except self._s3_error:
            return False

    async def is_file(self, path: str) -> bool:
        """Check if path is a file.

        Args:
            path: File path (relative to workspace)

        Returns:
            True if it's a file, False otherwise
        """
        try:
            stat = self.client.stat_object(self.bucket, path)
            return not stat.is_dir
        except self._s3_error:
            return False

    async def is_dir(self, path: str) -> bool:
        """Check if path is a directory.

        Args:
            path: Directory path (relative to workspace)

        Returns:
            True if it's a directory, False otherwise
        """
        # MinIO/S3 doesn't have real directories, but paths ending with /
        # are treated as directory prefixes
        return path.endswith("/")

    async def list(self, path: str = "") -> list[str]:
        """List files and directories under a path.

        Args:
            path: Directory path (relative to workspace, empty means root)

        Returns:
            List of relative paths
        """
        prefix = path if path else ""
        # Ensure prefix ends with / for directory listing
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        objects = self.client.list_objects(self.bucket, prefix=prefix, recursive=False)
        items = []
        for obj in objects:
            # Remove the prefix to get relative path
            relative_path = obj.object_name[len(prefix):] if prefix else obj.object_name
            if relative_path:
                items.append(relative_path)
        return items

    async def get_url(self, path: str, expires: int = 3600) -> str:
        """Get a URL for accessing the file.

        Args:
            path: File path (relative to workspace)
            expires: Expiration time for signed URL (seconds)

        Returns:
            Signed file access URL
        """
        return self.client.presigned_get_object(self.bucket, path, expires)
