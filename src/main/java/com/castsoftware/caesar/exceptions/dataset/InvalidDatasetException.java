/*
 * Copyright (C) 2020  Hugo JOBY
 *
 *  This library is free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as published by the Free Software Foundation; either version 3 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty ofnMERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNUnLesser General Public License v3 for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public v3 License along with this library; if not, write to the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 *
 */

package com.castsoftware.caesar.exceptions.dataset;

import com.castsoftware.caesar.exceptions.ExtensionException;

public class InvalidDatasetException extends ExtensionException {
  private static final long serialVersionUID = 5538686331898382229L;

  private static final String MESSAGE_PREFIX = "Error, the dataset seems to be corrupted : ";
  private static final String CODE_PREFIX = "DATS_CR_";

  public InvalidDatasetException(String message, String path, Throwable cause, String code) {
    super(
        MESSAGE_PREFIX.concat(message).concat(". Path : ").concat(path),
        cause,
        CODE_PREFIX.concat(code));
  }

  public InvalidDatasetException(String message, String code) {
    super(MESSAGE_PREFIX.concat(message), CODE_PREFIX.concat(code));
  }

  public InvalidDatasetException(String path, Throwable cause, String code) {
    super(MESSAGE_PREFIX.concat(path), cause, CODE_PREFIX.concat(code));
  }
}
