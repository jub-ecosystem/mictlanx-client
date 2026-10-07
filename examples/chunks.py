from mictlanx.utils.segmentation import Chunk,Chunks

def main():
    print("Creating a Chunks object with 3 chunks...")
    group_id = "group1"
    chunks = Chunks(
        chs = [
            Chunk(group_id = group_id,index=0, data = b"chunk1"), 
            Chunk(group_id = group_id, index=1, data = b"chunk2"), 
            Chunk(group_id = group_id, index=2, data = b"chunk3")
        ],
        n = 3,

    )

    print(f"Chunks: {chunks}")
    print(f"Number of chunks: {len(chunks)}")
    print(f"Total size: {chunks.size()} bytes")
    print(f"Chunk IDs: {[chunk.chunk_id for chunk in chunks]}")
    print(f"Chunk data: {[chunk.data for chunk in chunks]}")

if __name__ == "__main__":
    main()