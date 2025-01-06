import argparse
import json

def main():
    parser = argparse.ArgumentParser(description="category2depth/category3depth")
    parser.add_argument('category3depth', type=str, help='sexual')
    parser.add_argument('category4depth_list', type=str, help='category')
    
    args = parser.parse_args()

    category3depth = args.category3depth
    print(category3depth)
    category4depth_list = json.loads(args.category4depth_list)
    print(category4depth_list)

if __name__ == "__main__":
    main()