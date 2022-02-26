/*
Copyright 2022 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package validation

import (
	"bytes"
	"strings"

	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/kubernetes/pkg/apis/cdi"
	corevalidation "k8s.io/kubernetes/pkg/apis/core/validation"
)

var validateClaimName = apimachineryvalidation.NameIsDNSSubdomain

// ValidateResourceClass validates a ResourceClass.
func ValidateResourceClass(resourceClass *cdi.ResourceClass) field.ErrorList {
	allErrs := corevalidation.ValidateObjectMeta(&resourceClass.ObjectMeta, false, corevalidation.ValidateClassName, field.NewPath("metadata"))
	allErrs = append(allErrs, validateDriverName(resourceClass.DriverName, field.NewPath("driverName"))...)

	return allErrs
}

// ValidateResourceClassUpdate tests if an update to ResourceClass is valid.
func ValidateResourceClassUpdate(resourceClass, oldResourceClass *cdi.ResourceClass) field.ErrorList {
	allErrs := corevalidation.ValidateObjectMetaUpdate(&resourceClass.ObjectMeta, &oldResourceClass.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateResourceClass(resourceClass)...)
	return allErrs
}

// ValidateResourceClaim validates a ResourceClaim.
func ValidateResourceClaim(resourceClaim *cdi.ResourceClaim) field.ErrorList {
	allErrs := corevalidation.ValidateObjectMeta(&resourceClaim.ObjectMeta, true, validateClaimName, field.NewPath("metadata"))
	allErrs = append(allErrs, validateResourceClaimSpec(&resourceClaim.Spec, field.NewPath("spec"))...)
	allErrs = append(allErrs, validateResourceClaimStatus(&resourceClaim.Status, field.NewPath("status"))...)
	return allErrs
}

// ValidateResourceClaimUpdate tests if an update to ResourceClaim is valid.
func ValidateResourceClaimUpdate(resourceClaim, oldResourceClaim *cdi.ResourceClaim) field.ErrorList {
	allErrs := corevalidation.ValidateObjectMetaUpdate(&resourceClaim.ObjectMeta, &oldResourceClaim.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, validateResourceClaimSpecUnmodified(&resourceClaim.Spec, &oldResourceClaim.Spec, field.NewPath("spec"))...)
	allErrs = append(allErrs, ValidateResourceClaim(resourceClaim)...)
	return allErrs
}

// ValidateResourceClaimStatusUpdate tests if an update to the status of a ResourceClaim is valid.
func ValidateResourceClaimStatusUpdate(resourceClaim, oldResourceClaim *cdi.ResourceClaim) field.ErrorList {
	allErrs := corevalidation.ValidateObjectMetaUpdate(&resourceClaim.ObjectMeta, &oldResourceClaim.ObjectMeta, field.NewPath("metadata"))
	return allErrs
}

func validateResourceClaimSpec(spec *cdi.ResourceClaimSpec, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for _, msg := range corevalidation.ValidateClassName(spec.ResourceClassName, false) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("resourceClassName"), spec.ResourceClassName, msg))
	}
	return allErrs
}

func validateResourceClaimSpecUnmodified(spec, oldSpec *cdi.ResourceClaimSpec, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if spec.ResourceClassName != oldSpec.ResourceClassName {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("resourceClassName"), "updates are forbidden"))
	}
	if bytes.Compare(spec.Parameters.Raw, oldSpec.Parameters.Raw) != 0 {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("parameters"), "updates are forbidden"))
	}
	if spec.AllocationMode != oldSpec.AllocationMode {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("allocationMode"), "updates are forbidden"))
	}
	return allErrs
}

func validateResourceClaimStatus(status *cdi.ResourceClaimStatus, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	return allErrs
}

// validateDriverName tests if the resource driver name is a valid qualified name.
func validateDriverName(driverName string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if driverName == "" {
		allErrs = append(allErrs, field.Required(fldPath, driverName))
	} else {
		allErrs = append(allErrs, corevalidation.ValidateQualifiedName(strings.ToLower(driverName), fldPath)...)
	}
	return allErrs
}
