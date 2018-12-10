# Copyright 2018 Ben Walsh
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

Set-PSDebug -Trace 1

echo $env:Path

Get-Command python.exe

& cmake --help

$topdir = Get-Location

$janssondir = "$topdir\local_jansson"

New-Item $janssondir -itemType directory

Set-Location $janssondir

$janssonver = "2.12"
$janssontgz = "jansson-$janssonver.tar.gz"
$janssonurl = "http://www.digip.org/jansson/releases/$janssontgz"

(New-Object System.Net.WebClient).DownloadFile($janssonurl, "$janssondir\$janssontgz")

& tar -xzf $janssontgz

Set-Location "jansson-$janssonver"

& sed -e "s|/nologo||" -i CMakeLists.txt

New-Item build -itemType directory

Set-Location build

& cmake -G "NMake Makefiles" .. -DCMAKE_VERBOSE_MAKEFILE=ON -DCMAKE_INSTALL_PREFIX="$janssondir\jansson-$janssonver\dist" -DCMAKE_BUILD_TYPE=Release

& nmake
& nmake install

$env:CMAKE_PREFIX_PATH = "$janssondir\jansson-$janssonver\dist\cmake"

$avrodir = "$topdir\local_avro"

Set-Location $topdir

& git clone -b bw-win https://github.com/walshb/avro local_avro

Set-Location $avrodir

New-Item build -itemType directory

Set-Location build

& cmake -G "NMake Makefiles" ..\lang\c -DCMAKE_VERBOSE_MAKEFILE=ON -DCMAKE_INSTALL_PREFIX="$avrodir\dist" -DCMAKE_BUILD_TYPE=Release -DTHREADSAFE=true

& nmake

& nmake test

& nmake install

Set-Location $topdir

$env:PYAVROC_CFLAGS = "-I$avrodir\dist\include"
$env:PYAVROC_LIBS = "jansson"

$env:INCLUDE = "$avrodir\dist\include;$env:INCLUDE"
$env:LIB = "$avrodir\dist\lib;$env:LIB"

$env:INCLUDE = "$janssondir\jansson-$janssonver\dist\include;$env:INCLUDE"
$env:LIB = "$janssondir\jansson-$janssonver\dist\lib;$env:LIB"

echo $env:INCLUDE
echo $env:LIB

& python setup.py build

Set-Location "$topdir\local_avro\lang\py"

& python setup.py build

$pp1 = (Get-ChildItem -Path $topdir\build\lib*).FullName
$pp2 = (Get-ChildItem -Path $topdir\local_avro\lang\py\build\lib*).FullName

echo $pp1
echo $pp2

$env:PYTHONPATH = $pp1 + ';' + $pp2

echo $env:PYTHONPATH

Set-Location "$topdir\tests"

& python -m pytest -sv .
