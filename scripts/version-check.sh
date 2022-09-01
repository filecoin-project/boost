#!/usr/bin/env bash
set -ex

# Validate boost version matches the current tag
# $1 - boost path to execute
# $2 - boost git tag for this release
function validate_boost_version_matches_tag(){
  # sanity checks
  if [[ $# != 2 ]]; then
    echo "expected 2 args for validate_boost_version, got ${$#}"
    exit 100
  fi

  # extract version from `boost --version` response
  boost_path=$1
  # get version
  boost_raw_version=`${boost_path} --version`
  # grep for version string
  boost_actual_version=`echo ${boost_raw_version} | grep -oE '[0-9]+\.[0-9]+\.[0-9]+'`

  # trim leading 'v'
  tag=${2#v}
  # trim possible -rc[0-9]
  expected_version=${tag%-*}

  # check the versions are consistent
  if [[ ${expected_version} != ${boost_actual_version} ]]; then
    echo "boost version does not match build tag"
    exit 101
  fi
}

_boost_path=$1

if [[ ! -z "${CIRCLE_TAG}" ]]; then
  validate_boost_version_matches_tag "${_boost_path}" "${CIRCLE_TAG}"
else
  echo "No CI tag found. Skipping version check."
fi

