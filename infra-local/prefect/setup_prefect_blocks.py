#!/usr/bin/env python3
"""
setup_prefect_blocks.py

Автоматически настраивает блоки Prefect:
 - local-file-system (локальное хранилище)
 - process (локальная инфраструктура)
"""

import asyncio
from prefect.filesystems import LocalFileSystem
from prefect.infrastructure import Process
from prefect.client.orchestration import get_client
from prefect.exceptions import ObjectAlreadyExists


# === Настройки ===
PREFECT_API_URL = "http://127.0.0.1:4200/api"
LOCAL_STORAGE_PATH = "/home/notai/otus/kp_a2k/a2k-procar/infra-local/prefect"
STORAGE_BLOCK_NAME = "local-storage"
PROCESS_BLOCK_NAME = "local-process"


async def ensure_api_connection():
    """Проверяем соединение с API Prefect."""
    try:
        async with get_client() as client:
            await client.api_healthcheck()
        print(f"Prefect API доступен по адресу: {PREFECT_API_URL}")
    except Exception as e:
        print(f"Ошибка подключения к Prefect API ({PREFECT_API_URL}): {e}")
        print("Убедись, что Prefect сервер запущен командой: `prefect server start`")
        raise SystemExit(1)


async def create_local_storage_block():
    """Создание блока LocalFileSystem."""
    block = LocalFileSystem(basepath=LOCAL_STORAGE_PATH)
    try:
        await block.save(STORAGE_BLOCK_NAME, overwrite=False)
        print(f"Блок LocalFileSystem '{STORAGE_BLOCK_NAME}' создан (путь: {LOCAL_STORAGE_PATH})")
    except ObjectAlreadyExists:
        print(f"Блок LocalFileSystem '{STORAGE_BLOCK_NAME}' уже существует — пропускаем.")


async def create_process_block():
    """Создание блока Process (локальное выполнение)."""
    process_block = Process(
        command=["python"],
        stream_output=True,
        working_dir=None,
        env={},
    )
    try:
        await process_block.save(PROCESS_BLOCK_NAME, overwrite=False)
        print(f"Блок Process '{PROCESS_BLOCK_NAME}' создан.")
    except ObjectAlreadyExists:
        print(f"Блок Process '{PROCESS_BLOCK_NAME}' уже существует — пропускаем.")


async def main():
    print("Настройка блоков Prefect...\n")
    await ensure_api_connection()
    await create_local_storage_block()
    await create_process_block()
    print("\nВсе блоки успешно настроены!")
    print(f"Storage block: local-file-system/{STORAGE_BLOCK_NAME}")
    print(f"Infrastructure block: process/{PROCESS_BLOCK_NAME}")
    print("\nТеперь можно выполнять команду:\n")
    print(
        f"prefect deployment build train_pipeline_prefect.py:train_pipeline "
        f"-n 'train-car-price' -q 'default' "
        f"-sb local-file-system/{STORAGE_BLOCK_NAME} "
        f"-ib process/{PROCESS_BLOCK_NAME}"
    )


if __name__ == "__main__":
    asyncio.run(main())