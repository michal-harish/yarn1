package io.amient.yarn1;

/**
 * Yarn1 - Java Library for Apache YARN
 * Copyright (C) 2015 Michal Harish
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

public class YarnContainerRequest {
    public final Class<?> mainClass;
    public final String[] args;
    public final int priority;
    public final int directMemMb;
    public final int numCores;
    public final int heapMemMb;

    public YarnContainerRequest(Class<?> mainClass, String[] args, int priority, int directMemoryMb, int heapMemoryMb, int numCores) {
        this.mainClass = mainClass;
        this.args = args;
        this.priority = priority;
        this.directMemMb = directMemoryMb;
        this.heapMemMb = heapMemoryMb;
        this.numCores = numCores;
    }
}
