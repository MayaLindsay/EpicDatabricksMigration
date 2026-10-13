# Databricks notebook source
volume_path = "/Volumes/epicdatabricksimplementation/epic_raw/source_files/"

files = dbutils.fs.ls(volume_path)

display(files)