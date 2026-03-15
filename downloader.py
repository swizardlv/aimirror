"""
多线程分片下载器 - 支持 HTTP Range 并行下载和断点续传
"""
import os
import logging
import aiohttp
import aiofiles
import asyncio
from typing import Optional, List
from dataclasses import dataclass

@dataclass
class Chunk:
    start: int
    end: int
    downloaded: bool = False

class ParallelDownloader:
    def __init__(self, url: str, filepath: str, concurrency: int = 4,
                 chunk_size: int = 5*1024*1024, proxy: Optional[str] = None,
                 headers: Optional[dict] = None,
                 cache_manager = None, chunk_ttl_hours: int = 48):
        self.url = url
        self.filepath = filepath
        self.concurrency = concurrency
        self.chunk_size = chunk_size
        self.proxy = proxy
        self.headers = headers or {}
        self.total_size = 0
        self.chunks: List[Chunk] = []
        self.cache_manager = cache_manager  # 缓存管理器，用于断点续传
        self.chunk_ttl_hours = chunk_ttl_hours  # 分块缓存有效期（小时）
        
    async def _get_file_size(self, session: aiohttp.ClientSession) -> int:
        """获取文件总大小，检查是否支持 Range"""
        headers = dict(self.headers)
        async with session.head(self.url, headers=headers, allow_redirects=True, proxy=self.proxy) as resp:
            if resp.headers.get('Accept-Ranges') != 'bytes':
                raise ValueError(f"Server does not support Range requests: {self.url}")
            return int(resp.headers.get('Content-Length', 0))
    
    def _split_chunks(self, total_size: int) -> List[Chunk]:
        """分割下载范围"""
        # 自动模式：当 chunk_size <= 0 时，自动计算 chunk_size = 总大小 / concurrency
        if self.chunk_size <= 0:
            chunk_size = total_size // max((self.concurrency - 10), 1) # 避免除零错误,扣除10个线程，避免其他任务导致线程不够
            if chunk_size <= 0:
                chunk_size = total_size  # 防止除零或过小
        else:
            chunk_size = self.chunk_size
        
        chunks = []
        for start in range(0, total_size, chunk_size):
            end = min(start + chunk_size - 1, total_size - 1)
            chunks.append(Chunk(start=start, end=end))
        return chunks
    
    async def download_with_streaming(self, cache_key: str, temp_file: str, cache, content_type: str):
        """流式下载 - 边下载边写入文件，支持多个客户端同时读取和断点续传"""
        import aiofiles
        import asyncio
        
        try:
            logging.info(f"Starting streaming download: {self.url}")
            
            async with aiohttp.ClientSession() as session:
                # 1. 获取文件大小
                self.total_size = await self._get_file_size(session)
                logging.info(f"File size: {self.total_size / 1024 / 1024:.1f}MB")
                
                # 2. 预分配文件空间
                async with aiofiles.open(temp_file, 'wb') as f:
                    await f.truncate(self.total_size)
                
                # 3. 分割任务
                self.chunks = self._split_chunks(self.total_size)
                actual_chunk_size = self.chunks[0].end - self.chunks[0].start + 1 if self.chunks else 0
                logging.info(f"Split into {len(self.chunks)} chunks, actual chunk size: {actual_chunk_size / 1024 / 1024:.1f}MB")
                
                # 4. 检查已下载的分块（断点续传）
                if self.cache_manager:
                    cached_chunks = self.cache_manager.get_downloaded_chunks(
                        self.url, self.total_size, self.chunk_ttl_hours
                    )
                    for cached in cached_chunks:
                        for chunk in self.chunks:
                            if chunk.start == cached['start'] and chunk.end == cached['end']:
                                chunk.downloaded = True
                                break
                    
                    skipped = sum(1 for c in self.chunks if c.downloaded)
                    if skipped > 0:
                        logging.info(f"Resuming download: {skipped}/{len(self.chunks)} chunks already cached (within {self.chunk_ttl_hours}h)")
                
                # 5. 并发下载并实时写入（跳过已下载的）
                sem = asyncio.Semaphore(self.concurrency)
                pending_chunks = [c for c in self.chunks if not c.downloaded]
                
                async def download_and_write(chunk: Chunk):
                    """下载单个分片并立即写入文件"""
                    async with sem:
                        headers = dict(self.headers)
                        headers['Range'] = f'bytes={chunk.start}-{chunk.end}'
                        
                        for attempt in range(3):
                            try:
                                async with session.get(self.url, headers=headers, proxy=self.proxy) as resp:
                                    if resp.status == 206:
                                        data = await resp.read()
                                        # 立即写入文件
                                        async with aiofiles.open(temp_file, 'r+b') as f:
                                            await f.seek(chunk.start)
                                            await f.write(data)
                                        # 标记为已下载
                                        chunk.downloaded = True
                                        if self.cache_manager:
                                            self.cache_manager.mark_chunk_downloaded(
                                                self.url, self.total_size, chunk.start, chunk.end
                                            )
                                        return
                                    elif resp.status == 200 and chunk.start == 0:
                                        data = await resp.read()
                                        async with aiofiles.open(temp_file, 'r+b') as f:
                                            await f.write(data)
                                        chunk.downloaded = True
                                        if self.cache_manager:
                                            self.cache_manager.mark_chunk_downloaded(
                                                self.url, self.total_size, chunk.start, chunk.end
                                            )
                                        return
                                    else:
                                        raise RuntimeError(f"Chunk download failed: {resp.status}")
                            except Exception as e:
                                if attempt < 2:
                                    await asyncio.sleep(2 ** attempt)
                                else:
                                    raise
                
                if pending_chunks:
                    # 启动待下载任务
                    tasks = [download_and_write(chunk) for chunk in pending_chunks]
                    
                    # 等待所有下载完成
                    completed = sum(1 for c in self.chunks if c.downloaded)
                    failed_chunks = []
                    
                    for task in asyncio.as_completed(tasks):
                        try:
                            await task
                            completed += 1
                            if completed % max(1, len(self.chunks) // 10) == 0 or completed == len(self.chunks):
                                progress = completed / len(self.chunks) * 100
                                logging.info(f"Download progress: {progress:.1f}% ({completed}/{len(self.chunks)} chunks)")
                        except Exception as e:
                            logging.error(f"Chunk download failed: {e}")
                            failed_chunks.append(e)
                    
                    # 检查是否有失败的 chunk
                    if failed_chunks:
                        raise RuntimeError(f"Download failed: {len(failed_chunks)}/{len(pending_chunks)} chunks failed")
                else:
                    logging.info("All chunks already downloaded, skipping download")
                
                logging.info(f"Streaming download completed: {temp_file}")
                
                # 6. 验证文件完整性
                actual_size = os.path.getsize(temp_file)
                if actual_size != self.total_size:
                    raise RuntimeError(f"File size mismatch: expected {self.total_size}, got {actual_size}")
                
                # 7. 存入缓存
                cache.put(cache_key, temp_file, content_type)
                
                # 8. 下载成功，清除 chunk 缓存
                if self.cache_manager:
                    self.cache_manager.clear_chunks_for_url(self.url)
                    logging.info("Cleared chunk cache after successful download")
                
        except Exception as e:
            logging.error(f"Streaming download failed: {e}")
            if os.path.exists(temp_file):
                os.remove(temp_file)
            raise
        finally:
            # 清理 active_downloads
            import main
            if cache_key in main.active_downloads:
                del main.active_downloads[cache_key]
