#!/bin/bash
#SBATCH --job-name=cluster_pieces
#SBATCH --account=project_2005072
#SBATCH -o %x.log
#SBATCH --time=40:00
#SBATCH --ntasks=1
#SBATCH --nodes=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=180G
#SBATCH --partition=small

module load go
go run main.go
