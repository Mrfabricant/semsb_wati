from setuptools import setup, find_packages

with open("requirements.txt") as f:
	install_requires = f.read().strip().split("\n")

setup(
	name="semsb_wati",
	version="0.0.1",
	description="SEMSB WATI WhatsApp Integration",
	author="SRRI Easwari Mills",
	packages=find_packages(),
	zip_safe=False,
	include_package_data=True,
	install_requires=install_requires,
)