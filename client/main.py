from client.uploader import ChunkedUploader


def main() -> None:
    for result in ChunkedUploader().sync_once():
        print(f"{result['status']}: {result['path']}")


if __name__ == "__main__":
    main()
